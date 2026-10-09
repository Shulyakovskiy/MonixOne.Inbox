using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Processing;

internal sealed class InboxProcessor<TDbContext>(
    TDbContext context,
    IServiceProvider services,
    IServiceScopeFactory scopes,
    InboxCatalog<TDbContext> catalog,
    InboxDiagnostics diagnostics,
    InboxMetrics metrics,
    InboxRunningKeys running,
    ILogger<InboxProcessor<TDbContext>> logger
)
    where TDbContext : DbContext
{
    private static readonly JsonSerializerOptions _jsonOptions = new() { MaxDepth = 128 };

    private static readonly string _version = typeof(InboxResult)
        .Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!
        .InformationalVersion;
    private string Schema => catalog.Settings.Schema;
    private Guid _decisionId;
    private bool _gapClosed;

    internal async Task<bool> ProcessOneAsync(
        InboxHandlerRegistration<TDbContext> handler, string worker, CancellationToken token)
    {
        InboxStateRow? state = null;
        InboxRow? row = null;
        InboxAttemptRow? attempt = null;
        InboxProcessingKey? acquired = null;
        var decisionId = _decisionId = Guid.NewGuid();
        var committing = false;
        try
        {
            var processed = await new SingleExecution(context).ExecuteAsync(async cancellation =>
            {
                await using var transaction = await context.Database.BeginTransactionAsync(
                    IsolationLevel.ReadCommitted, cancellation);
                await using var data = context.CreateLinqToDBConnection(transaction);
                if (context.Database.GetCommandTimeout() is { } timeout)
                    data.CommandTimeout = timeout;
                var candidates = await CandidatesAsync(data, handler, cancellation);
                foreach (var candidate in candidates)
                {
                    var key = new InboxProcessingKey(handler.Id, candidate.Producer,
                        candidate.SequenceScope, candidate.ObjectKey);
                    if (!running.TryAdd(key))
                        continue;
                    acquired = key;
                    state = candidate;
                    row = await data.GetTable<InboxRow>().SchemaName(Schema)
                        .SingleOrDefaultAsyncLinqToDB(x => x.Id == candidate.InboxId, cancellation);
                    if (row is null)
                        throw new CoordinationLostException();
                    if (row.Sequence <= state.LastSequence)
                    {
                        await CoordinateAsync(data, state, row, advance: false, cancellation);
                        await SkipStaleAsync(data, state, row, worker, cancellation);
                    }
                    else if (row.Status == "pending" && row.Code == "terminal_policy_transition")
                    {
                        await CoordinateAsync(data, state, row, advance: true, cancellation);
                        row.Status = "dead";
                        row.CompletedAt = DateTimeOffset.UtcNow;
                        await WriteInboxAsync(data, row, cancellation);
                        await JournalAsync(data, row, worker, "terminal_policy_transition", null, null,
                            new { last_sequence = row.Sequence }, cancellation);
                        await CloseRangeAsync(data, state, row, worker, candidate.FirstReceivedAt, cancellation);
                    }
                    else
                        attempt = await ApplyAsync(data, transaction, state, row, handler, worker,
                            candidate.FirstReceivedAt, cancellation);

                    committing = true;
                    await transaction.CommitAsync(cancellation);
                    committing = false;
                    return true;
                }
                return false;
            }, token);
            if (processed)
                ObserveCommitted(state!, row!, attempt, worker);
            return processed;
        }
        catch (Exception) when (committing && state is not null && row is not null && !token.IsCancellationRequested)
        {
            // Dispose/rollback the old transaction before reading immutable proof on the primary.
            await VerifyCommitAsync(state, row, decisionId, attempt?.AttemptId, worker, token);
            return true;
        }
        catch (Exception error) when (error is CoordinationLostException || CoordinationFailure(error))
        {
            context.ChangeTracker.Clear();
            await Task.Delay(Random.Shared.Next(25, 76), token);
            return false;
        }
        finally
        {
            // Never release the key before the cooperative callback actually returns.
            if (acquired is not null)
                running.Remove(acquired);
        }
    }

    internal IQueryable<Candidate> CandidatesQuery(DataConnection data,
        InboxHandlerRegistration<TDbContext> handler)
    {
        var active = data.GetTable<InboxRow>().SchemaName(Schema)
            .Where(x => x.HandlerId == handler.Id && (x.Status == "pending" || x.Status == "retry"));
        foreach (var key in running.Snapshot(handler.Id))
            active = active.Where(x => x.Producer != key.Producer ||
                x.SequenceScope != key.Scope || x.ObjectKey != key.ObjectKey);

        // DistinctBy translates to PostgreSQL DISTINCT ON; the window includes every active row of the key.
        var heads = active.Select(x => new
            {
                x.Id, x.Producer, x.SequenceScope, x.ObjectKey, x.Sequence, x.NextAttemptAt,
                FirstReceivedAt = Sql.Ext.Min(x.ReceivedAt).Over()
                    .PartitionBy(x.Producer, x.SequenceScope, x.ObjectKey).ToValue(),
            })
            .OrderBy(x => x.Producer).ThenBy(x => x.SequenceScope).ThenBy(x => x.ObjectKey).ThenBy(x => x.Sequence)
            .DistinctBy(x => new { x.Producer, x.SequenceScope, x.ObjectKey }).AsCte("heads");
        var states = data.GetTable<InboxStateRow>().SchemaName(Schema);
        var candidates = from head in heads
            join state in states on new { HandlerId = handler.Id, head.Producer, head.SequenceScope, head.ObjectKey }
                equals new { state.HandlerId, state.Producer, state.SequenceScope, state.ObjectKey }
            where head.Sequence <= state.LastSequence ||
                ((head.NextAttemptAt == null || head.NextAttemptAt <= InboxPostgreSql.TransactionTimestamp()) &&
                 ((state.LastSequence == null && head.FirstReceivedAt <= InboxPostgreSql.Subtract(
                       InboxPostgreSql.TransactionTimestamp(), handler.Processing.StartupReorderWindow)) ||
                  (state.LastSequence != null && ((decimal)head.Sequence == (decimal)state.LastSequence + 1m ||
                       head.FirstReceivedAt <= InboxPostgreSql.Subtract(
                           InboxPostgreSql.TransactionTimestamp(), handler.Processing.GapTimeout)))))
            orderby state.UpdatedAt, head.FirstReceivedAt, head.Id
            select new Candidate
            {
                InboxId = head.Id, HandlerId = state.HandlerId, Producer = state.Producer,
                SequenceScope = state.SequenceScope, ObjectKey = state.ObjectKey,
                LastSequence = state.LastSequence, Version = state.Version,
                UpdatedAt = state.UpdatedAt, FirstReceivedAt = head.FirstReceivedAt,
            };
        return candidates.Take(32);
    }

    private Task<List<Candidate>> CandidatesAsync(DataConnection data,
        InboxHandlerRegistration<TDbContext> handler, CancellationToken token) =>
        CandidatesQuery(data, handler).ToListAsyncLinqToDB(token);

    internal sealed class Candidate : InboxStateRow
    {
        internal Guid InboxId { get; init; }
        internal DateTimeOffset FirstReceivedAt { get; init; }
    }

    private sealed class CoordinationLostException : Exception;

    private static bool CoordinationFailure(Exception error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException)
            if (current is PostgresException { SqlState: "55P03" or "40P01" or "40001" })
                return true;
        return false;
    }

    private async Task CoordinateAsync(DataConnection data, InboxStateRow state, InboxRow row,
        bool advance, CancellationToken token)
    {
        // Applied business writes have already completed. This timeout affects coordination only.
        await InboxPostgreSql.SetLockTimeoutAsync(data, token);
        var inbox = data.GetTable<InboxRow>().SchemaName(Schema);
        var changed = await data.GetTable<InboxStateRow>().SchemaName(Schema)
            .Where(s => s.HandlerId == state.HandlerId && s.Producer == state.Producer &&
                s.SequenceScope == state.SequenceScope && s.ObjectKey == state.ObjectKey &&
                s.Version == state.Version &&
                (s.LastSequence == state.LastSequence || (s.LastSequence == null && state.LastSequence == null)) &&
                inbox.Any(i => i.Id == row.Id && i.RunId == row.RunId && i.AttemptCount == row.AttemptCount &&
                    (i.Status == "pending" || i.Status == "retry")) &&
                !inbox.Any(earlier => earlier.HandlerId == s.HandlerId && earlier.Producer == s.Producer &&
                    earlier.SequenceScope == s.SequenceScope && earlier.ObjectKey == s.ObjectKey &&
                    (earlier.Status == "pending" || earlier.Status == "retry") && earlier.Sequence < row.Sequence))
            .Set(s => s.Version, s => s.Version + 1)
            .Set(s => s.UpdatedAt, _ => InboxPostgreSql.ClockTimestamp())
            .Set(s => s.LastSequence, s => advance ? row.Sequence : s.LastSequence)
            .UpdateAsync(token);
        if (changed != 1)
            throw new CoordinationLostException();
    }

    private async Task CloseRangeAsync(DataConnection data, InboxStateRow state, InboxRow row,
        string worker, DateTimeOffset firstReceivedAt, CancellationToken token)
    {
        if (state.LastSequence is null)
            await JournalAsync(data, row, worker, "initial_sequence_selected", null, null,
                new { previous_cursor = (long?)null, selected_sequence = row.Sequence }, token);
        else if (row.Sequence - 1 > state.LastSequence)
        {
            _gapClosed = true;
            await JournalAsync(data, row, worker, "gap_skipped", null, null,
                new { previous_cursor = state.LastSequence, first_missing = state.LastSequence + 1,
                    last_missing = row.Sequence - 1, selected_sequence = row.Sequence,
                    waited_ms = Math.Max(0, (DateTimeOffset.UtcNow - firstReceivedAt).TotalMilliseconds) }, token);
        }
        state.LastSequence = row.Sequence;
        state.Version++;
    }

    private void ObserveCommitted(InboxStateRow state, InboxRow row, InboxAttemptRow? attempt, string worker)
    {
        metrics.Processing(Schema, row, attempt);
        if (_gapClosed)
            metrics.Decision(Schema, row.HandlerId, row.SubscriptionId, "gap_skipped");
        logger.Log(
            row.Status is "dead" or "retry" ? LogLevel.Warning : LogLevel.Debug,
            "OrderedInbox committed {Status}: {HandlerId}/{SubscriptionId}, inbox {InboxId}, "
                + "attempt {AttemptId}, number {AttemptNumber}, code {Code}, "
                + "cursor {LastSequence}, next retry {NextAttemptAt}, worker {WorkerInstanceId}.",
            row.Status,
            row.HandlerId,
            row.SubscriptionId,
            row.Id,
            attempt?.AttemptId,
            row.AttemptCount,
            row.Code,
            state.LastSequence,
            row.NextAttemptAt,
            worker
        );
    }

    private async Task SkipStaleAsync(
        DataConnection data,
        InboxStateRow state,
        InboxRow row,
        string worker,
        CancellationToken token
    )
    {
        row.Status = "skipped";
        row.Code = "stale_sequence";
        row.Reason = "Sequence is already closed by the consumer cursor.";
        row.CompletedAt = DateTimeOffset.UtcNow;
        row.NextAttemptAt = null;
        await WriteInboxAsync(data, row, token);
        await JournalAsync(
            data,
            row,
            worker,
            "stale_sequence",
            null,
            null,
            new { last_sequence = state.LastSequence, actual_sequence = row.Sequence },
            token
        );
    }

    private async Task<InboxAttemptRow?> ApplyAsync(
        DataConnection data,
        IDbContextTransaction transaction,
        InboxStateRow state,
        InboxRow row,
        InboxHandlerRegistration<TDbContext> handler,
        string worker,
        DateTimeOffset firstReceivedAt,
        CancellationToken token
    )
    {
        if (row.AttemptCount >= handler.Processing.Retry.MaxAttempts)
        {
            // A lower configured budget after restart cannot grant an extra callback.
            await FinishAsync(
                data,
                state,
                row,
                handler,
                worker,
                InboxResult.Reject("Business attempt budget is already exhausted.", "retry_exhausted"),
                null,
                firstReceivedAt,
                token
            );
            return null;
        }
        var attempt = new InboxAttemptRow
        {
            AttemptId = Guid.NewGuid(),
            InboxId = row.Id,
            RunId = row.RunId,
            AttemptNumber = checked(row.AttemptCount + 1),
            WorkerInstanceId = worker,
            HandlerType = handler.HandlerType.FullName!,
            StartedAt = DateTimeOffset.UtcNow,
            CorrelationId = Header(row.Source, "x-correlation-id"),
            TraceId = TraceId(row.Source),
        };
        using var logScope = logger.BeginScope(
            new Dictionary<string, object?>
            {
                ["HandlerId"] = handler.Id,
                ["SubscriptionId"] = row.SubscriptionId,
                ["InboxId"] = row.Id,
                ["AttemptId"] = attempt.AttemptId,
                ["RunId"] = row.RunId,
                ["WorkerInstanceId"] = worker,
                ["CorrelationId"] = attempt.CorrelationId,
                ["TraceId"] = attempt.TraceId,
                ["Sequence"] = row.Sequence,
            }
        );
        diagnostics.Begin(attempt.AttemptId);
        await transaction.CreateSavepointAsync("business", token);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(handler.Processing.HandlerTimeout);
        var timer = Stopwatch.StartNew();
        InboxResult result;
        try
        {
            using var json = JsonDocument.Parse(row.Envelope);
            result =
                await handler.Resolve(services).HandleAsync(context, json.RootElement, deadline.Token)
                ?? throw new InvalidOperationException("Business handler returned null.");
            token.ThrowIfCancellationRequested();
            deadline.Token.ThrowIfCancellationRequested();
            if (result.Kind == InboxResultKind.Applied)
                await context.SaveChangesAsync(deadline.Token);
            deadline.Token.ThrowIfCancellationRequested();
            attempt.Outcome = result.Kind switch
            {
                InboxResultKind.Applied => "applied",
                InboxResultKind.Ignore => "ignored",
                InboxResultKind.Retry => "retry",
                _ => "rejected",
            };
        }
        catch (Exception error) when (!token.IsCancellationRequested)
        {
            var timedOut = deadline.IsCancellationRequested;
            if (CoordinationFailure(error) || !timedOut && InfrastructureFailure(error))
                throw;
            attempt.Exception = error.ToString().Replace("\0", "\\0", StringComparison.Ordinal);
            result = timedOut
                ? InboxResult.Retry("Handler deadline exceeded.", code: "handler_timeout")
                : Classify(error, handler.Processing);
            attempt.Outcome =
                timedOut ? "timeout"
                : result.Kind == InboxResultKind.Reject ? "rejected"
                : "retry";
        }

        token.ThrowIfCancellationRequested();
        // Await the cooperative callback to completion; a timeout never starts a parallel replacement invocation.
        if (result.Kind != InboxResultKind.Applied)
        {
            await transaction.RollbackToSavepointAsync("business", token);
            context.ChangeTracker.Clear();
        }
        else
            await transaction.ReleaseSavepointAsync("business", token);
        timer.Stop();
        attempt.DurationMs = timer.ElapsedMilliseconds;
        var finishedAt = DateTimeOffset.UtcNow;
        attempt.FinishedAt = finishedAt < attempt.StartedAt ? attempt.StartedAt : finishedAt;
        attempt.Code = result.Code;
        attempt.Reason = result.Reason;
        attempt.Details = InboxDiagnosticJson.Bound(result.Details);
        attempt.Diagnostics = diagnostics.Complete();
        // Invalid diagnostic JSON must not roll back an otherwise successful business result.
        if (attempt.Diagnostics != "[]")
            attempt.Diagnostics = await ValidateDiagnosticJsonAsync(data, transaction, attempt.Diagnostics, token);
        if (attempt.Details is not null)
            attempt.Details = await ValidateDiagnosticJsonAsync(data, transaction, attempt.Details, token);
        await FinishAsync(data, state, row, handler, worker, result, attempt, firstReceivedAt, token);
        return attempt;
    }

    private InboxResult Classify(Exception error, InboxProcessingSettings settings)
    {
        if (settings.ClassifyException is { } classify)
        {
            try
            {
                var result = classify(error);
                if (result is null || result.Kind is not (InboxResultKind.Retry or InboxResultKind.Reject))
                    throw new InvalidOperationException("ClassifyException must return Retry or Reject.");
                return result;
            }
            catch (Exception classificationError)
            {
                diagnostics.Write(
                    "exception_classifier.failed",
                    "Exception classification failed; the original business error will be retried.",
                    level: LogLevel.Warning,
                    exception: classificationError
                );
            }
        }
        return InboxResult.Retry("Business handler failed.", code: "handler_exception");
    }

    private static async Task<string> ValidateDiagnosticJsonAsync(
        DataConnection data,
        IDbContextTransaction transaction,
        string json,
        CancellationToken token
    )
    {
        await transaction.CreateSavepointAsync("diagnostic_json", token);
        try
        {
            await data.SelectAsync(() => InboxPostgreSql.ValidateJson(json), token);
            return json;
        }
        catch (PostgresException error) when (error.SqlState.StartsWith("22", StringComparison.Ordinal))
        {
            await transaction.RollbackToSavepointAsync("diagnostic_json", token);
            // Diagnostic storage failures must not roll back an otherwise valid business outcome.
            var fallback = new
            {
                code = "diagnostics.invalid_jsonb",
                sqlstate = error.SqlState,
            };
            return json.StartsWith('[')
                ? JsonSerializer.Serialize(new[] { fallback })
                : JsonSerializer.Serialize(fallback);
        }
        finally
        {
            await transaction.ReleaseSavepointAsync("diagnostic_json", token);
        }
    }

    private async Task FinishAsync(
        DataConnection data,
        InboxStateRow state,
        InboxRow row,
        InboxHandlerRegistration<TDbContext> handler,
        string worker,
        InboxResult result,
        InboxAttemptRow? attempt,
        DateTimeOffset firstReceivedAt,
        CancellationToken token
    )
    {
        var terminal = result.Kind == InboxResultKind.Reject ||
            result.Kind == InboxResultKind.Retry
            && (attempt?.AttemptNumber ?? row.AttemptCount) >= handler.Processing.Retry.MaxAttempts;
        var advance = result.Kind is InboxResultKind.Applied or InboxResultKind.Ignore || terminal;
        await CoordinateAsync(data, state, row, advance, token);
        if (attempt is not null)
        {
            row.AttemptCount = attempt.AttemptNumber;
            row.CurrentAttemptId = attempt.AttemptId;
        }
        row.Status = terminal
            ? "dead"
            : result.Kind switch
            {
                InboxResultKind.Applied => "processed",
                InboxResultKind.Ignore => "skipped",
                _ => "retry",
            };
        row.Code = result.Kind == InboxResultKind.Retry && terminal ? "retry_exhausted" : result.Code;
        row.Reason = result.Reason;
        row.CompletedAt = row.Status == "retry" ? null : DateTimeOffset.UtcNow;
        row.NextAttemptAt =
            row.Status == "retry"
                ? DateTimeOffset.UtcNow + RetryDelay(handler.Processing.Retry, row.AttemptCount, result.RetryAfter)
                : null;
        Guid? dlqId = null;
        if (terminal)
        {
            dlqId = Guid.NewGuid();
            await data.InsertAsync(
                new InboxDlqRow
                {
                    Id = dlqId.Value,
                    InboxId = row.Id,
                    RunId = row.RunId,
                    HandlerId = row.HandlerId,
                    SubscriptionId = row.SubscriptionId,
                    DeduplicationKey = $"terminal:{row.Id:N}:{row.RunId:N}",
                    Category = "terminal",
                    EventId = row.EventId,
                    Producer = row.Producer,
                    SequenceScope = row.SequenceScope,
                    ObjectKey = row.ObjectKey,
                    Sequence = row.Sequence,
                    EventType = row.EventType,
                    Code = row.Code,
                    Reason = row.Reason!,
                    Exception = attempt?.Exception,
                    LastSequence = state.LastSequence,
                    DlqPolicy = "SkipAndAdvance",
                    CursorAdvanced = advance,
                    WorkerInstanceId = worker,
                    HandlerType = handler.HandlerType.FullName!,
                    PackageVersion = _version,
                },
                schemaName: Schema,
                token: token
            );
        }
        if (advance)
            await CloseRangeAsync(data, state, row, worker, firstReceivedAt, token);
        else
            state.Version++;
        if (attempt is not null)
        {
            attempt.NextAttemptAt = row.NextAttemptAt;
            await data.InsertAsync(attempt, schemaName: Schema, token: token);
        }
        await WriteInboxAsync(data, row, token);
        await JournalAsync(
            data,
            row,
            worker,
            row.Status,
            attempt?.AttemptId,
            dlqId,
            new
            {
                row.Code,
                row.Reason,
                next_attempt_at = row.NextAttemptAt,
                last_sequence = state.LastSequence,
                cursor_advanced = advance,
            },
            token
        );
    }

    private Task<int> WriteInboxAsync(DataConnection data, InboxRow row, CancellationToken token) =>
        data.GetTable<InboxRow>()
            .SchemaName(Schema)
            .Where(x => x.Id == row.Id)
            .Set(x => x.Status, row.Status)
            .Set(x => x.AttemptCount, row.AttemptCount)
            .Set(x => x.CurrentAttemptId, row.CurrentAttemptId)
            .Set(x => x.NextAttemptAt, row.NextAttemptAt)
            .Set(x => x.CompletedAt, row.CompletedAt)
            .Set(x => x.Code, row.Code)
            .Set(x => x.Reason, row.Reason)
            .Set(x => x.UpdatedAt, DateTimeOffset.UtcNow)
            .UpdateAsync(token);

    private Task<int> JournalAsync(
        DataConnection data,
        InboxRow row,
        string worker,
        string decision,
        Guid? attempt,
        Guid? dlq,
        object details,
        CancellationToken token
    ) =>
        data.InsertAsync(
            new InboxJournalRow
            {
                InboxId = row.Id,
                HandlerId = row.HandlerId,
                SubscriptionId = row.SubscriptionId,
                WorkerInstanceId = worker,
                Decision = decision,
                DecisionId = decision is "processed" or "skipped" or "retry" or "dead" or
                    "stale_sequence" or "terminal_policy_transition" ? _decisionId : null,
                AttemptId = attempt,
                DlqId = dlq,
                Details = JsonSerializer.Serialize(details, _jsonOptions),
            },
            schemaName: Schema,
            token: token
        );

    private async Task VerifyCommitAsync(InboxStateRow original, InboxRow row, Guid decisionId,
        Guid? attemptId, string worker, CancellationToken token)
    {
        var failures = 0;
        while (true)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
                await new SingleExecution(db).ExecuteAsync(async cancellation =>
                {
                    await using var transaction = await db.Database.BeginTransactionAsync(cancellation);
                    await using var data = db.CreateLinqToDBConnection(transaction);
                    await InboxPostgreSql.SetLockTimeoutAsync(data, cancellation);
                    // A short write settles a still-completing coordinator before reading its unique decision id.
                    // No callback or explicit row lock occurs during verification.
                    await data.GetTable<InboxStateRow>().SchemaName(Schema)
                        .Where(x => x.HandlerId == original.HandlerId && x.Producer == original.Producer &&
                            x.SequenceScope == original.SequenceScope && x.ObjectKey == original.ObjectKey)
                        .Set(x => x.Version, x => x.Version).UpdateAsync(cancellation);
                    var committed = await data.GetTable<InboxJournalRow>().SchemaName(Schema)
                        .AnyAsyncLinqToDB(x => x.DecisionId == decisionId, cancellation);
                    var attempt = committed && attemptId is not null
                        ? await data.GetTable<InboxAttemptRow>().SchemaName(Schema)
                            .SingleOrDefaultAsyncLinqToDB(x => x.AttemptId == attemptId, cancellation)
                        : null;
                    var status = attempt is null ? row.Status : await data.GetTable<InboxJournalRow>()
                        .SchemaName(Schema).Where(x => x.AttemptId == attemptId &&
                            (x.Decision == "processed" || x.Decision == "skipped" || x.Decision == "retry" ||
                                x.Decision == "dead")).Select(x => x.Decision).SingleAsyncLinqToDB(cancellation);
                    await transaction.RollbackAsync(cancellation);
                    if (committed)
                    {
                        metrics.Processing(Schema, row, attempt, status);
                        if (_gapClosed)
                            metrics.Decision(Schema, row.HandlerId, row.SubscriptionId, "gap_skipped");
                    }
                    logger.LogInformation("OrderedInbox COMMIT checked: decision {DecisionId}, inbox {InboxId}, " +
                        "committed {Committed}, worker {WorkerInstanceId}.", decisionId, row.Id, committed, worker);
                }, token);
                return;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                failures = Math.Min(failures + 1, 6);
                logger.LogWarning("OrderedInbox COMMIT remains unknown ({ErrorType}), decision {DecisionId}; " +
                    "callback is not repeated during verification.", error.GetType().Name, decisionId);
                await Task.Delay(TimeSpan.FromSeconds(1 << (failures - 1)), token);
            }
        }
    }

    internal static TimeSpan RetryDelay(InboxRetrySettings settings, int attemptNumber, TimeSpan? explicitDelay)
    {
        var milliseconds =
            explicitDelay?.TotalMilliseconds
            ?? Math.Min(
                settings.MaxDelay.TotalMilliseconds,
                settings.InitialDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attemptNumber - 1, 62))
            );
        if (explicitDelay is null)
            milliseconds *= 0.5 + Random.Shared.NextDouble() * 0.5;
        return TimeSpan.FromMilliseconds(Math.Clamp(milliseconds, 1, settings.MaxDelay.TotalMilliseconds));
    }

    private static bool InfrastructureFailure(Exception error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException)
            if (current is NpgsqlException { IsTransient: true })
                return true;
        return false;
    }

    private static string? Header(string source, string name)
    {
        using var json = JsonDocument.Parse(source);
        if (!json.RootElement.TryGetProperty("headers", out var headers) || headers.ValueKind != JsonValueKind.Object)
            return null;
        foreach (var property in headers.EnumerateObject())
            if (
                property.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
                && property.Value.ValueKind == JsonValueKind.Array
                && property.Value.GetArrayLength() > 0
                && property.Value[0].ValueKind == JsonValueKind.String
            )
                return property.Value[0].GetString();
        return null;
    }

    private static string? TraceId(string source)
    {
        var parts = Header(source, "traceparent")?.Split('-');
        return parts is { Length: 4 } && parts[1].Length == 32 ? parts[1] : null;
    }

    // This strategy sets EF's current execution scope so nested EF commands do not retry independently.
    private sealed class SingleExecution(DbContext db) : ExecutionStrategy(db, 0, TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => false;
    }
}

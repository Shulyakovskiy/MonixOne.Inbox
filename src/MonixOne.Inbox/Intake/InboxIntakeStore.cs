using System.Data;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Intake;

internal sealed record InboxDelivery(byte[] Raw, string Source, string Identity);

internal sealed record IntakeDecision(string Kind, Guid[] InboxIds, Guid? DlqId)
{
    internal Guid? InboxId => InboxIds.Length == 0 ? null : InboxIds[0];
}

internal sealed class InboxIntakeStore<TDbContext>(
    TDbContext context,
    InboxCatalog<TDbContext> catalog,
    InboxMetrics metrics,
    IServiceProvider services
)
    where TDbContext : DbContext
{
    private static readonly string _packageVersion = typeof(InboxIntakeStore<>)
        .Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!
        .InformationalVersion;

    internal async Task<IntakeDecision> SaveAsync(
        InboxHandlerRegistration<TDbContext> handler,
        InboxSubscriptionSettings subscription,
        InboxDelivery delivery,
        string workerId,
        CancellationToken token
    )
    {
        var parsed = InboxEnvelopeParser.Parse(delivery.Raw, services.GetService<IInboxEnvelopeAdapter>());
        // Only durable, idempotent intake is retried here. No NATS ACK or business callback is in this delegate.
        var decision = await context
            .Database.CreateExecutionStrategy()
            .ExecuteAsync(
                async cancellation =>
                {
                    await using var transaction = await context.Database.BeginTransactionAsync(
                        IsolationLevel.ReadCommitted,
                        cancellation
                    );
                    await using var data = context.CreateLinqToDBConnection(transaction);
                    if (context.Database.GetCommandTimeout() is { } timeout)
                        data.CommandTimeout = timeout;
                    await InboxPostgreSql.SetLockTimeoutAsync(data, cancellation);
                    var current = await ValidateJsonAsync(data, transaction, parsed, cancellation);

                    var result = await SaveEnvelopeAsync(
                        data,
                        handler,
                        subscription,
                        delivery,
                        workerId,
                        current,
                        cancellation
                    );

                    // One receipt path for accepted, duplicate, invalid and conflicting messages.
                    // A cross-key conflict links every original; invalid messages have no inbox id.
                    foreach (var inboxId in result.InboxIds.Select(id => (Guid?)id).DefaultIfEmpty())
                        await data.InsertAsync(
                            new InboxJournalRow
                            {
                                HandlerId = handler.Id,
                                SubscriptionId = subscription.Id,
                                WorkerInstanceId = workerId,
                                Decision = result.Kind,
                                InboxId = inboxId,
                                DlqId = result.DlqId,
                                Details = delivery.Source,
                            },
                            schemaName: catalog.Settings.Schema,
                            token: cancellation
                        );

                    await transaction.CommitAsync(cancellation);
                    return result;
                },
                token
            );
        metrics.Decision(catalog.Settings.Schema, handler.Id, subscription.Id, decision.Kind);
        return decision;
    }

    private async Task<IntakeDecision> SaveEnvelopeAsync(
        DataConnection data,
        InboxHandlerRegistration<TDbContext> handler,
        InboxSubscriptionSettings subscription,
        InboxDelivery delivery,
        string worker,
        ParsedInboxEvent parsed,
        CancellationToken token
    )
    {
        if (parsed.Metadata is not null)
            return await SaveValidAsync(data, handler, subscription, delivery, worker, parsed, token);
        var dlq = await SaveDlqAsync(
            data,
            handler,
            subscription,
            delivery,
            worker,
            parsed,
            "invalid_message",
            null,
            null,
            token
        );
        return new("invalid_message", [], dlq);
    }

    private async Task<IntakeDecision> SaveValidAsync(
        DataConnection data,
        InboxHandlerRegistration<TDbContext> handler,
        InboxSubscriptionSettings subscription,
        InboxDelivery delivery,
        string worker,
        ParsedInboxEvent parsed,
        CancellationToken token
    )
    {
        var schema = catalog.Settings.Schema;
        var metadata = parsed.Metadata!;
        var rows = data.GetTable<InboxRow>().SchemaName(schema);
        // Existing keys are read without touching their row. An insert race is handled by uniqueness.
        if (!await data.GetTable<InboxStateRow>().SchemaName(schema).AnyAsyncLinqToDB(x =>
            x.HandlerId == handler.Id && x.Producer == metadata.Producer &&
            x.SequenceScope == metadata.SequenceScope && x.ObjectKey == metadata.ObjectKey, token))
        {
            data.NextQueryHints.Add("ON CONFLICT DO NOTHING");
            await data.InsertAsync(new InboxStateRow
            {
                HandlerId = handler.Id,
                Producer = metadata.Producer,
                SequenceScope = metadata.SequenceScope,
                ObjectKey = metadata.ObjectKey,
            }, schemaName: schema, token: token);
        }

        var id = Guid.NewGuid();
        data.NextQueryHints.Add("ON CONFLICT DO NOTHING");
        var inserted = await data.InsertAsync(new InboxRow
        {
            Id = id,
            HandlerId = handler.Id,
            SubscriptionId = subscription.Id,
            EventId = metadata.EventId,
            Producer = metadata.Producer,
            SequenceScope = metadata.SequenceScope,
            ObjectKey = metadata.ObjectKey,
            Sequence = metadata.Sequence,
            EventType = metadata.EventType,
            OccurredAt = metadata.OccurredAt,
            Envelope = parsed.Json!,
            RawPayload = delivery.Raw,
            Source = delivery.Source,
            RunId = Guid.NewGuid(),
        }, schemaName: schema, token: token);
        if (inserted == 1)
            return new("received", [id], null);

        // A new statement sees the unique-index winner under ReadCommitted.
        var originals = await rows.Where(x => x.HandlerId == handler.Id &&
            (x.EventId == metadata.EventId || (x.Producer == metadata.Producer &&
                x.SequenceScope == metadata.SequenceScope && x.ObjectKey == metadata.ObjectKey &&
                x.Sequence == metadata.Sequence))).OrderBy(x => x.Id).ToListAsyncLinqToDB(token);
        if (originals.Count == 0)
            throw new InvalidOperationException("Intake identity changed during cleanup; redelivery must retry.");

        if (
            originals.Count == 1
            && SameMetadata(originals[0], metadata)
            && await rows.Where(x => x.Id == originals[0].Id)
                .Select(x => InboxPostgreSql.JsonEquals(x.Envelope, parsed.Json!))
                .SingleAsyncLinqToDB(token)
        )
        {
            return new("duplicate", [originals[0].Id], null);
        }

        var conflict = await SaveDlqAsync(
            data,
            handler,
            subscription,
            delivery,
            worker,
            parsed with
            {
                Error = "Different facts share the same event_id or sequence key and number.",
            },
            "conflict",
            originals[0].Id,
            null,
            token
        );
        return new("conflict", originals.Select(x => x.Id).ToArray(), conflict);
    }

    private async Task<Guid> SaveDlqAsync(
        DataConnection data,
        InboxHandlerRegistration<TDbContext> handler,
        InboxSubscriptionSettings subscription,
        InboxDelivery delivery,
        string worker,
        ParsedInboxEvent parsed,
        string category,
        Guid? relatedId,
        long? cursor,
        CancellationToken token
    )
    {
        var schema = catalog.Settings.Schema;
        var dedup = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(delivery.Identity)));
        var existing = await data.GetTable<InboxDlqRow>()
            .SchemaName(schema)
            .Where(x => x.HandlerId == handler.Id && x.DeduplicationKey == dedup)
            .SingleOrDefaultAsyncLinqToDB(token);
        if (existing is not null)
            return existing.Id;
        var metadata = parsed.Metadata;
        var row = new InboxDlqRow
        {
            Id = Guid.NewGuid(),
            RelatedInboxId = relatedId,
            HandlerId = handler.Id,
            SubscriptionId = subscription.Id,
            DeduplicationKey = dedup,
            Category = category,
            EventId = metadata?.EventId,
            Producer = metadata?.Producer,
            SequenceScope = metadata?.SequenceScope,
            ObjectKey = metadata?.ObjectKey,
            Sequence = metadata?.Sequence,
            EventType = metadata?.EventType,
            Envelope = parsed.Json,
            RawPayload = delivery.Raw,
            Source = delivery.Source,
            Code = category,
            Reason = parsed.Error!,
            LastSequence = cursor,
            WorkerInstanceId = worker,
            HandlerType = handler.HandlerType.FullName!,
            PackageVersion = _packageVersion,
        };
        data.NextQueryHints.Add("ON CONFLICT (handler_id, deduplication_key) DO NOTHING");
        if (await data.InsertAsync(row, schemaName: schema, token: token) == 1)
            return row.Id;
        // ReadCommitted sees the winning delivery in a separate statement after an insert race.
        return await data.GetTable<InboxDlqRow>().SchemaName(schema)
            .Where(x => x.HandlerId == handler.Id && x.DeduplicationKey == dedup)
            .Select(x => x.Id).SingleAsyncLinqToDB(token);
    }

    private static async Task<ParsedInboxEvent> ValidateJsonAsync(
        DataConnection data,
        IDbContextTransaction transaction,
        ParsedInboxEvent parsed,
        CancellationToken token
    )
    {
        if (parsed.Json is null)
            return parsed;

        // PostgreSQL jsonb rejects syntactically valid JSON containing NUL or out-of-range numbers.
        // A savepoint lets intake retain its original bytes in DLQ within the same transaction.
        await transaction.CreateSavepointAsync("envelope", token);
        try
        {
            await data.SelectAsync(() => InboxPostgreSql.ValidateJson(parsed.Json), token);
            await transaction.ReleaseSavepointAsync("envelope", token);
            return parsed;
        }
        catch (PostgresException error) when (error.SqlState.StartsWith("22", StringComparison.Ordinal))
        {
            await transaction.RollbackToSavepointAsync("envelope", token);
            return new(null, null, $"Envelope cannot be stored as jsonb ({error.SqlState}).");
        }
    }

    private static bool SameMetadata(InboxRow row, InboxEventMetadata value) =>
        row.EventId == value.EventId
        && row.Producer == value.Producer
        && row.SequenceScope == value.SequenceScope
        && row.ObjectKey == value.ObjectKey
        && row.Sequence == value.Sequence
        && row.EventType == value.EventType
        && row.OccurredAt == value.OccurredAt;
}

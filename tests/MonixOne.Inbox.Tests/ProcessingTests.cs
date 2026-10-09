using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Text.Json;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Migrations;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Npgsql;

namespace MonixOne.Inbox.Tests;

[Collection("PostgreSQL")]
public sealed partial class ProcessingTests(PostgreSqlFixture infrastructure) : IAsyncLifetime
{
    private readonly string _schema = $"processing_{Guid.NewGuid():N}";
    private readonly string _stream = $"EVENTS_{Guid.NewGuid():N}";
    private readonly string _subject = $"test.{Guid.NewGuid():N}";
    private readonly List<IHost> _hosts = [];
    private readonly Probe _probe = new();
    private NatsConnection _nats = null!;
    private NatsJSContext _js = null!;
    private CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _nats = new(NatsOpts.Default with { Url = infrastructure.NatsUrl });
        _js = new(_nats);
        await _js.CreateStreamAsync(new StreamConfig(_stream, [$"{_subject}.>"]), cancellationToken: Token);
        LinqToDBForEFTools.Initialize();
        await using var db = new TestDb(
            new DbContextOptionsBuilder<TestDb>().UseNpgsql(infrastructure.ConnectionString).Options
        );
        await InboxSchemaMigrator.EnsureReadyAsync(db, _schema, true, NullLogger.Instance, Token);
        // Ordinary application tables are deliberately outside the package schema/model.
        await SqlAsync(
            "CREATE TABLE public.processing_business (\"Id\" uuid PRIMARY KEY, \"Key\" text NOT NULL, "
                + "\"Sequence\" bigint NOT NULL, \"Kind\" text NOT NULL); "
                + "CREATE TABLE public.processing_outbox (event_id uuid PRIMARY KEY)"
        );
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in _hosts)
        {
            await host.StopAsync(Token);
            host.Dispose();
        }
        await _js.DeleteStreamAsync(_stream, Token);
        await _nats.DisposeAsync();
        await SqlAsync(
            $"DROP SCHEMA \"{_schema}\" CASCADE; "
                + "DROP TABLE public.processing_business; DROP TABLE public.processing_outbox"
        );
    }

    [Fact]
    public async Task Applied_commits_tracked_changes_sql_intermediate_save_outbox_and_cursor_in_one_transaction()
    {
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.False(await ProcessAsync(host));
        Assert.Equal(3, await BusinessCountAsync());
        Assert.Equal(1, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 1"));
        Assert.Equal(1, await CountAsync("inbox", "status = 'processed' AND attempt_count = 1"));
        Assert.Equal(
            1,
            await CountAsync(
                "inbox_attempts",
                "outcome = 'applied' AND finished_at IS NOT NULL "
                    + "AND duration_ms >= 0 AND correlation_id = 'correlation-1' "
                    + "AND trace_id = '0123456789abcdef0123456789abcdef'"
            )
        );
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'processed' AND attempt_id IS NOT NULL"));
        Assert.Single(_probe.Calls);
    }

    [Theory]
    [InlineData("ignore", "skipped", "ignored", true)]
    [InlineData("retry", "retry", "retry", false)]
    [InlineData("reject", "dead", "rejected", true)]
    public async Task Non_applied_results_roll_back_every_business_write(
        string result,
        string status,
        string outcome,
        bool advance
    )
    {
        _probe.Handle = (_, _, _) =>
            Task.FromResult(
                result switch
                {
                    "ignore" => InboxResult.Ignore("No business effect", "ignored_type"),
                    "retry" => InboxResult.Retry("Dependency missing", TimeSpan.FromSeconds(20), "dependency_missing"),
                    _ => InboxResult.Reject("Invalid business event", "invalid_business_event"),
                }
            );
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(1, await CountAsync("inbox", $"status = '{status}' AND attempt_count = 1"));
        Assert.Equal(1, await CountAsync("inbox_attempts", $"outcome = '{outcome}'"));
        Assert.Equal(1, await CountAsync("consumer_state", advance ? "last_sequence = 1" : "last_sequence IS NULL"));
        Assert.Equal(result == "reject" ? 1 : 0, await CountAsync("inbox_dlq"));
    }

    [Fact]
    public async Task Explicit_retry_delay_is_bounded_and_result_details_are_retained()
    {
        _probe.Handle = (_, _, _) =>
        {
            using var details = JsonDocument.Parse("{\"dependency\":\"profile\"}");
            return Task.FromResult(
                InboxResult.Retry("Dependency missing", TimeSpan.FromDays(1), "dependency", details.RootElement)
            );
        };
        var host = HostFor(configure: h => h.Retry.MaxDelay = TimeSpan.FromSeconds(3));
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        var seconds = await ScalarAsync<double>(
            $"SELECT extract(epoch FROM next_attempt_at - updated_at)::float8 " + $"FROM \"{_schema}\".inbox"
        );
        Assert.InRange(seconds, 2.5, 3);
        Assert.Equal(1, await CountAsync("inbox_attempts", "details ->> 'dependency' = 'profile'"));
        Assert.False(await ProcessAsync(host));
    }

    [Fact]
    public async Task Lower_retry_budget_after_restart_finishes_without_extra_business_invocation()
    {
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Retry("Wait", TimeSpan.FromMilliseconds(1)));
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        await Task.Delay(5, Token);
        var restarted = HostFor(configure: h => h.Retry.MaxAttempts = 1);
        Assert.True(await ProcessAsync(restarted));
        Assert.Single(_probe.Calls);
        Assert.Equal(1, await CountAsync("inbox_attempts"));
        Assert.Equal(1, await CountAsync("inbox_dlq", "code = 'retry_exhausted' AND cursor_advanced"));
    }

    [Fact]
    public async Task Earliest_retry_blocks_later_sequence_but_not_another_object_and_survives_new_provider()
    {
        var first = true;
        _probe.Handle = (_, _, _) =>
            Task.FromResult(first ? InboxResult.Retry("Wait", TimeSpan.FromSeconds(20)) : InboxResult.Applied);
        var host = HostFor();
        await SeedAsync(host, 1);
        await SeedAsync(host, 2);
        await SeedAsync(host, 1, "other");
        Assert.True(await ProcessAsync(host));
        first = false;
        var restarted = HostFor();
        Assert.True(await ProcessAsync(restarted)); // The ready other object gets progress.
        Assert.False(await ProcessAsync(restarted));
        Assert.Equal(1, await CountAsync("inbox", "object_key = 'object-1' AND sequence = 2 AND status = 'pending'"));
        await SqlAsync(
            $"UPDATE \"{_schema}\".inbox SET next_attempt_at = now() - interval '1 second' " + "WHERE status = 'retry'"
        );
        Assert.True(await ProcessAsync(restarted));
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(2, await CountAsync("consumer_state", "last_sequence > 0"));
        Assert.Equal(4, _probe.Calls.Count);
    }

    [Fact]
    public async Task Exhausted_retry_preserves_history_and_advances()
    {
        _probe.Handle = (_, message, _) =>
            Task.FromResult(
                message.GetProperty("object_key").GetString() == "other"
                || message.GetProperty("sequence").GetInt64() == 2
                    ? InboxResult.Applied
                    : InboxResult.Retry("Dependency missing", TimeSpan.FromMilliseconds(1), "dependency")
            );
        var host = HostFor(configure: h =>
        {
            h.Retry.MaxAttempts = 3;
        });
        await SeedAsync(host, 1);
        await SeedAsync(host, 2);
        for (var i = 0; i < 3; i++)
        {
            Assert.True(await ProcessAsync(host));
            await Task.Delay(5, Token);
        }
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(3, await CountAsync("inbox_attempts", "outcome = 'retry'"));
        Assert.Equal(
            1,
            await CountAsync(
                "inbox_dlq",
                "category = 'terminal' AND inbox_id IS NOT NULL "
                    + "AND run_id IS NOT NULL AND code = 'retry_exhausted'"
            )
        );
        Assert.Equal(
            3,
            await ScalarAsync<long>(
                $"SELECT count(*) FROM \"{_schema}\".inbox_attempts a "
                    + $"JOIN \"{_schema}\".inbox_dlq d ON d.inbox_id = a.inbox_id AND d.run_id = a.run_id"
            )
        );
        Assert.Equal(
            1,
            await CountAsync(
                "consumer_state",
                "last_sequence = 1"
            )
        );
        Assert.True(await ProcessAsync(host));
        await SeedAsync(host, 1, "other");
        Assert.True(await ProcessAsync(host));
        Assert.Equal(6, await BusinessCountAsync());
        Assert.Equal(1, await CountAsync("inbox_dlq"));
    }

    [Fact]
    public async Task Failure_writing_final_journal_rolls_back_applied_business_and_keeps_event_available()
    {
        var host = HostFor();
        await SeedAsync(host, 1);
        await SqlAsync(
            $"""
            CREATE FUNCTION "{_schema}".fail_result() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.decision = 'processed' THEN
                    RAISE EXCEPTION 'Simulated journal failure';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER fail_result BEFORE INSERT ON "{_schema}".inbox_journal
                FOR EACH ROW EXECUTE FUNCTION "{_schema}".fail_result();
            """
        );
        await Assert.ThrowsAsync<PostgresException>(() => ProcessAsync(host));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.Equal(1, await CountAsync("inbox", "status = 'pending' AND attempt_count = 0"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL"));
        await SqlAsync($"DROP TRIGGER fail_result ON \"{_schema}\".inbox_journal");
        Assert.True(await ProcessAsync(HostFor()));
        Assert.Equal(3, await BusinessCountAsync());
        Assert.Equal(1, await CountAsync("inbox_attempts"));
    }

    [Fact]
    public async Task PostgreSql_statement_error_rolls_back_to_savepoint_before_recording_retry()
    {
        _probe.Handle = async (db, _, ct) =>
        {
            await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO public.processing_outbox SELECT event_id " + "FROM public.processing_outbox",
                ct
            );
            return InboxResult.Applied;
        };
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(1, await CountAsync("inbox_attempts", "outcome = 'retry' AND exception LIKE '%23505%'"));
        Assert.Equal(1, await CountAsync("inbox", "status = 'retry' AND attempt_count = 1"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Timeout_rolls_back_and_waits_for_callback_completion_even_if_token_is_ignored(bool cooperative)
    {
        _probe.Handle = async (_, _, ct) =>
        {
            await Task.Delay(250, cooperative ? ct : Token);
            return InboxResult.Applied;
        };
        var logs = new LogCapture();
        var host = HostFor(
            configure: h =>
            {
                h.HandlerTimeout = TimeSpan.FromMilliseconds(100);
                h.ClassifyException = UnexpectedClassification;
            },
            logs: logs
        );
        await SeedAsync(host, 1);
        var timer = Stopwatch.StartNew();
        Assert.True(await ProcessAsync(host));
        if (!cooperative)
            Assert.True(timer.Elapsed >= TimeSpan.FromMilliseconds(250));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(1, await CountAsync("inbox_attempts", "outcome = 'timeout' AND code = 'handler_timeout'"));
        Assert.DoesNotContain(
            logs.Records,
            x => x.Fields.GetValueOrDefault("DiagnosticCode") as string == "exception_classifier.failed"
        );
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL"));
    }

    [Fact]
    public async Task Shutdown_cancellation_rolls_back_without_consuming_attempt_budget_and_restart_applies()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Handle = async (_, _, ct) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return InboxResult.Applied;
        };
        var logs = new LogCapture();
        var host = HostFor(configure: h => h.ClassifyException = UnexpectedClassification, logs: logs);
        await SeedAsync(host, 1);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var processing = ProcessAsync(host, stop.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => processing);
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.DoesNotContain(
            logs.Records,
            x => x.Fields.GetValueOrDefault("DiagnosticCode") as string == "exception_classifier.failed"
        );
        Assert.Equal(1, await CountAsync("inbox", "status = 'pending' AND attempt_count = 0"));
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        Assert.True(await ProcessAsync(HostFor()));
        Assert.Equal(3, await BusinessCountAsync());
    }

    [Fact]
    public async Task Connection_loss_in_callback_rolls_back_without_business_retry_and_fresh_scope_recovers()
    {
        _probe.Handle = async (db, _, ct) =>
        {
            var connection = db.Database.GetDbConnection();
            await using var command = connection.CreateCommand();
            command.Transaction = db.Database.CurrentTransaction!.GetDbTransaction();
            command.CommandText = "SELECT pg_backend_pid()";
            var pid = (int)(await command.ExecuteScalarAsync(ct))!;
            await SqlAsync($"SELECT pg_terminate_backend({pid})");
            await db.Database.ExecuteSqlRawAsync("SELECT 1", ct);
            return InboxResult.Applied;
        };
        var logs = new LogCapture();
        var host = HostFor(configure: h => h.ClassifyException = UnexpectedClassification, logs: logs);
        await SeedAsync(host, 1);
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => ProcessAsync(host));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.DoesNotContain(
            logs.Records,
            x => x.Fields.GetValueOrDefault("DiagnosticCode") as string == "exception_classifier.failed"
        );
        Assert.Equal(1, await CountAsync("inbox", "attempt_count = 0 AND status = 'pending'"));
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        Assert.True(await ProcessAsync(HostFor()));
        Assert.Equal(2, _probe.Calls.Count); // No nested EF retry replayed the callback.
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Commit_failure_is_verified_with_fresh_context_without_blind_callback_replay(bool committed)
    {
        var failure = new CommitFailure(committed);
        var host = HostFor(interceptor: failure);
        await SeedAsync(host, 1);
        failure.Armed = true;
        Assert.True(await ProcessAsync(host));
        Assert.Single(_probe.Calls);
        Assert.Equal(committed ? 3 : 0, await BusinessCountAsync());
        Assert.Equal(committed ? 1 : 0, await CountAsync("inbox_attempts"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        Assert.Equal(!committed, await ProcessAsync(host));
        Assert.Equal(3, await BusinessCountAsync());
        Assert.Equal(1, await CountAsync("inbox_attempts"));
    }

    [Fact]
    public async Task Stale_and_bigint_boundary_events_do_not_overflow_or_call_business_again()
    {
        var host = HostFor();
        await SeedAsync(host, long.MaxValue);
        await SqlAsync($"UPDATE \"{_schema}\".consumer_state SET last_sequence = 9223372036854775806");
        Assert.True(await ProcessAsync(host));
        Assert.Single(_probe.Calls);
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.False(await ProcessAsync(host));
        Assert.Single(_probe.Calls);
        Assert.Equal(1, await CountAsync("inbox", "code = 'stale_sequence' AND attempt_count = 0"));
        Assert.Equal(long.MaxValue, await ScalarAsync<long>($"SELECT last_sequence FROM \"{_schema}\".consumer_state"));
    }

    [Fact]
    public async Task Live_transport_two_handlers_two_subscriptions_and_three_replicas_apply_once_in_order()
    {
        var hosts = Enumerable.Range(0, 3).Select(_ => HostFor(dispatch: true, secondHandler: true, configure: h => h.StartupReorderWindow = TimeSpan.FromMilliseconds(100))).ToArray();
        await Task.WhenAll(hosts.Select(h => h.StartAsync(Token)));
        foreach (var sequence in new long[] { 3, 1, 2 })
            (
                await _js.PublishAsync(
                    $"{_subject}.{(sequence == 2 ? "b" : "a")}",
                    Event(sequence),
                    serializer: NatsRawSerializer<byte[]>.Default,
                    cancellationToken: Token
                )
            ).EnsureSuccess();
        await UntilAsync(async () => await CountAsync("inbox", "status = 'processed'") == 6);
        Assert.Equal(6, await CountAsync("inbox_attempts"));
        Assert.Equal(2, await CountAsync("consumer_state", "last_sequence = 3"));
        Assert.Equal(18, await BusinessCountAsync());
        foreach (var id in new[] { "locations", "products" })
        {
            var committed = await ScalarAsync<string>($"SELECT json_agg(i.sequence ORDER BY j.id)::text " +
                $"FROM \"{_schema}\".inbox_journal j JOIN \"{_schema}\".inbox i ON i.id = j.inbox_id " +
                $"WHERE j.decision = 'processed' AND i.handler_id = '{id}'");
            using var json = JsonDocument.Parse(committed);
            Assert.Equal(new long[] { 1, 2, 3 }, json.RootElement.EnumerateArray().Select(x => x.GetInt64()));
        }
        foreach (var id in new[] { "locations", "products", "locations-other", "products-other" })
            await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, id, Token)).Info.NumAckPending == 0);
    }

    [Theory]
    [InlineData("applied")]
    [InlineData("ignore")]
    [InlineData("retry")]
    [InlineData("reject")]
    public async Task Business_diagnostics_are_committed_with_each_result_and_correlated_in_ILogger(string outcome)
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            using var details = JsonDocument.Parse("{\"profile_id\":42}");
            diagnostics.Write("projection.prepared", "Projection checked.", details.RootElement);
            return Task.CompletedTask;
        };
        _probe.Handle = (_, _, _) =>
            Task.FromResult(
                outcome switch
                {
                    "applied" => InboxResult.Applied,
                    "ignore" => InboxResult.Ignore("No effect"),
                    "retry" => InboxResult.Retry("Wait"),
                    _ => InboxResult.Reject("Invalid event"),
                }
            );
        var logs = new LogCapture();
        var host = HostFor(logs: logs);
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(
            1,
            await CountAsync(
                "inbox_attempts",
                "jsonb_array_length(diagnostics) = 1 "
                    + "AND diagnostics -> 0 ->> 'code' = 'projection.prepared' "
                    + "AND diagnostics -> 0 -> 'details' ->> 'profile_id' = '42'"
            )
        );
        var attempt = await ScalarAsync<Guid>($"SELECT attempt_id FROM \"{_schema}\".inbox_attempts");
        var log = Assert.Single(logs.Records, x => x.Category == typeof(InboxDiagnostics).FullName);
        Assert.Equal(attempt, log.Fields["AttemptId"]);
        Assert.Equal("locations", log.Fields["HandlerId"]);
        Assert.Equal("events", log.Fields["SubscriptionId"]);
        Assert.Equal("correlation-1", log.Fields["CorrelationId"]);
        Assert.Equal("0123456789abcdef0123456789abcdef", log.Fields["TraceId"]);
        Assert.Equal(1L, log.Fields["Sequence"]);
        Assert.Equal(outcome == "applied" ? 3 : 0, await BusinessCountAsync());
        Assert.Null(Assert.Single(_probe.Scopes).AttemptId);
    }

    [Fact]
    public async Task ILogger_filters_do_not_remove_durable_business_diagnostics()
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            diagnostics.Write("projection.checked", "Check completed.");
            return Task.CompletedTask;
        };
        var logs = new LogCapture();
        var host = HostFor(logs: logs, disableDiagnosticLogs: true);
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.DoesNotContain(logs.Records, x => x.Category == typeof(InboxDiagnostics).FullName);
        Assert.Equal(1, await CountAsync("inbox_attempts", "diagnostics -> 0 ->> 'code' = 'projection.checked'"));
    }

    [Fact]
    public async Task Parallel_objects_have_separate_scoped_diagnostics_and_attempt_log_fields()
    {
        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        _probe.Diagnose = async (diagnostics, message, ct) =>
        {
            diagnostics.Write("object.checked", "Object checked.", message);
            if (Interlocked.Increment(ref entered) == 2)
                both.TrySetResult();
            await both.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        };
        var logs = new LogCapture();
        var host = HostFor(logs: logs);
        await SeedAsync(host, 1, "a");
        await SeedAsync(host, 1, "b");
        Assert.All(await Task.WhenAll(ProcessAsync(host), ProcessAsync(host)), Assert.True);
        Assert.Equal(2, _probe.Scopes.Distinct().Count());
        Assert.Equal(
            2,
            await ScalarAsync<long>(
                $"SELECT count(*) FROM \"{_schema}\".inbox_attempts a "
                    + $"JOIN \"{_schema}\".inbox i ON i.id = a.inbox_id "
                    + "WHERE jsonb_array_length(a.diagnostics) = 1 "
                    + "AND a.diagnostics -> 0 -> 'details' ->> 'object_key' = i.object_key"
            )
        );
        var records = logs.Records.Where(x => x.Category == typeof(InboxDiagnostics).FullName).ToArray();
        Assert.Equal(2, records.Select(x => x.Fields["AttemptId"]).Distinct().Count());
        Assert.Equal(2, records.Select(x => x.Fields["InboxId"]).Distinct().Count());
        foreach (var scope in _probe.Scopes)
            Assert.Throws<ObjectDisposedException>(() => scope.Write("late", "The operation scope is disposed."));
    }

    [Fact]
    public async Task Retry_and_terminal_DLQ_keep_diagnostics_and_full_errors_of_all_scopes()
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            diagnostics.Write("dependency.checked", "Attempt dependency checked.");
            return Task.CompletedTask;
        };
        _probe.Handle = (_, _, _) =>
            throw new InvalidOperationException("Business failed.", new ArgumentException("Inner error."));
        var host = HostFor(configure: h =>
        {
            h.Retry.MaxAttempts = 2;
            h.Retry.InitialDelay = TimeSpan.FromMilliseconds(1);
            h.Retry.MaxDelay = TimeSpan.FromMilliseconds(1);
        });
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        await Task.Delay(5, Token);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(2, _probe.Scopes.Distinct().Count());
        Assert.Equal(
            2,
            await ScalarAsync<long>(
                $"SELECT count(*) FROM \"{_schema}\".inbox_attempts a "
                    + $"JOIN \"{_schema}\".inbox_dlq d ON d.inbox_id = a.inbox_id AND d.run_id = a.run_id "
                    + "WHERE a.diagnostics -> 0 ->> 'code' = 'dependency.checked' "
                    + "AND a.exception LIKE '%Inner error.%' AND a.exception LIKE '%Handler.HandleAsync%'"
            )
        );
        Assert.Equal(0, await BusinessCountAsync());
    }

    [Fact]
    public async Task Permanent_exception_classifier_rejects_and_retains_original_error_and_details()
    {
        _probe.Handle = (_, _, _) => throw new ArgumentException("Unsupported business value.");
        var host = HostFor(configure: h =>
        {
            h.ClassifyException = static error =>
            {
                using var details = JsonDocument.Parse("{\"field\":\"name\"}");
                return error is ArgumentException
                    ? InboxResult.Reject("Unsupported business value.", "invalid_value", details.RootElement)
                    : InboxResult.Retry("Unexpected error");
            };
        });
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(
            1,
            await CountAsync(
                "inbox_attempts",
                "outcome = 'rejected' AND code = 'invalid_value' "
                    + "AND details ->> 'field' = 'name' AND exception LIKE '%Unsupported business value.%' "

            )
        );
        Assert.Equal(
            1,
            await CountAsync("inbox_dlq", "code = 'invalid_value' AND exception LIKE '%ArgumentException%'")
        );
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(
            1,
            await CountAsync(
                "consumer_state",
                "last_sequence = 1"
            )
        );
    }

    [Theory]
    [InlineData("throw")]
    [InlineData("applied")]
    [InlineData("ignore")]
    [InlineData("null")]
    public async Task Broken_classifier_falls_back_to_retry_and_records_both_errors(string mode)
    {
        _probe.Handle = (_, _, _) => throw new ArgumentException("Original business error.");
        var host = HostFor(configure: h =>
            h.ClassifyException = mode switch
            {
                "throw" => static _ => throw new InvalidOperationException("Classifier failed."),
                "applied" => static _ => InboxResult.Applied,
                "ignore" => static _ => InboxResult.Ignore("Wrong decision"),
                _ => static _ => null!,
            }
        );
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(
            1,
            await CountAsync(
                "inbox_attempts",
                "outcome = 'retry' AND code = 'handler_exception' "
                    + "AND exception LIKE '%Original business error.%' "
                    + "AND diagnostics -> 0 ->> 'code' = 'exception_classifier.failed' "
                    + "AND diagnostics -> 0 ->> 'exception' IS NOT NULL"
            )
        );
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await CountAsync("inbox_dlq"));
    }

    [Theory]
    [InlineData("{\"value\":\"\\u0000\"}")]
    [InlineData("{\"value\":1e9999999}")]
    public async Task Unsupported_jsonb_diagnostics_and_result_details_use_safe_fallback_without_changing_decision(
        string raw
    )
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            using var details = JsonDocument.Parse(raw);
            diagnostics.Write("invalid_jsonb", "Keep original diagnostic JSON.", details.RootElement);
            return Task.CompletedTask;
        };
        _probe.Handle = (_, _, _) =>
        {
            using var details = JsonDocument.Parse(raw);
            return Task.FromResult(InboxResult.Ignore("No effect", details: details.RootElement));
        };
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        var snapshot = await ScalarAsync<string>($"SELECT diagnostics::text FROM \"{_schema}\".inbox_attempts");
        using var json = JsonDocument.Parse(snapshot);
        var entry = Assert.Single(json.RootElement.EnumerateArray());
        Assert.Equal("diagnostics.invalid_jsonb", entry.GetProperty("code").GetString());
        Assert.Equal(1, await CountAsync("inbox_attempts", "details ->> 'code' = 'diagnostics.invalid_jsonb'"));
        Assert.Equal(1, await CountAsync("inbox", "status = 'skipped' AND attempt_count = 1"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 1"));
    }

    [Fact]
    public async Task Standard_depth_result_and_diagnostic_JSON_fit_wrapped_history_and_keep_business_decision()
    {
        var raw = string.Concat(Enumerable.Repeat("{\"nested\":", 64)) + "42" + new string('}', 64);
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            using var details = JsonDocument.Parse(raw);
            diagnostics.Write("deep", "Full diagnostic data.", details.RootElement);
            return Task.CompletedTask;
        };
        _probe.Handle = (_, _, _) =>
        {
            using var details = JsonDocument.Parse(raw);
            return Task.FromResult(InboxResult.Ignore("No effect", details: details.RootElement));
        };
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("inbox", "status = 'skipped' AND attempt_count = 1"));
        Assert.Equal(1, await CountAsync("inbox_attempts", "diagnostics -> 0 ->> 'code' = 'deep'"));
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'skipped'"));
    }

    [Fact]
    public async Task Large_diagnostic_buffer_truncation_does_not_reject_or_retry_applied_event()
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            for (var i = 0; i < 500; i++)
                diagnostics.Write($"step.{i}", new string('x', 1024));
            return Task.CompletedTask;
        };
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("inbox", "status = 'processed'"));
        Assert.Equal(1, await CountAsync("inbox_attempts", "diagnostics -> -1 ->> 'code' = 'diagnostics.truncated'"));
        Assert.Equal(3, await BusinessCountAsync());
    }

    private static InboxResult UnexpectedClassification(Exception error) =>
        throw new InvalidOperationException(
            "Protected timeout/cancellation/infrastructure errors must not be classified."
        );

    private IHost HostFor(
        bool dispatch = false,
        bool secondHandler = false,
        Action<InboxHandlerOptions>? configure = null,
        IInterceptor? interceptor = null,
        LogCapture? logs = null,
        bool disableDiagnosticLogs = false,
        Action<OrderedInboxOptions>? configureGlobal = null,
        Action<IServiceCollection>? configureServices = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        if (logs is not null)
        {
            builder.Logging.AddProvider(logs).SetMinimumLevel(LogLevel.Trace);
            if (disableDiagnosticLogs)
                builder.Logging.AddFilter("MonixOne.Inbox.InboxDiagnostics", LogLevel.None);
        }
        builder.Services.AddSingleton(_probe);
        builder.Services.AddScoped<Dependency>();
        builder.Services.AddDbContext<TestDb>(options =>
        {
            options.UseNpgsql(infrastructure.ConnectionString, p => p.EnableRetryOnFailure());
            if (interceptor is not null)
                options.AddInterceptors(interceptor);
        });
        builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(
            NatsOpts.Default with
            {
                Url = infrastructure.NatsUrl,
            }
        ));
        var inbox = builder.Services.AddOrderedInbox<TestDb>(o =>
        {
            o.Schema = _schema;
            o.AutoMigrate = false;
            configureGlobal?.Invoke(o);
        });
        foreach (var id in secondHandler ? new[] { "locations", "products" } : ["locations"])
        {
            void Configure(InboxHandlerOptions h)
            {
                h.Subscribe("events", _stream, $"{_subject}.a", id);
                h.Subscribe("other", _stream, $"{_subject}.b", $"{id}-other");
                h.PollInterval = TimeSpan.FromMilliseconds(10);
                h.StartupReorderWindow = TimeSpan.Zero;
                h.GapTimeout = TimeSpan.Zero;
                configure?.Invoke(h);
            }
            if (id == "locations")
                inbox.AddHandler<LocationsHandler>(id, Configure);
            else
                inbox.AddHandler<ProductsHandler>(id, Configure);
        }
        if (!dispatch)
            builder.Services.Remove(
                builder.Services.Single(d => d.ImplementationType == typeof(InboxDispatcher<TestDb>))
            );
        configureServices?.Invoke(builder.Services);
        var host = builder.Build();
        _hosts.Add(host);
        return host;
    }

    private async Task SeedAsync(IHost host, long sequence, string key = "object-1", string subscription = "events")
    {
        var handler = host
            .Services.GetRequiredService<InboxCatalog<TestDb>>()
            .Handlers.Single(h => h.Id == "locations");
        await using var scope = host.Services.CreateAsyncScope();
        await scope
            .ServiceProvider.GetRequiredService<InboxIntakeStore<TestDb>>()
            .SaveAsync(
                handler,
                handler.Subscriptions.Single(s => s.Id == subscription),
                new InboxDelivery(
                    Event(sequence, key),
                    "{\"headers\":{\"x-correlation-id\":[\"correlation-1\"],"
                        + "\"traceparent\":[\"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\"]}}",
                    Guid.NewGuid().ToString("N")
                ),
                "intake-worker",
                Token
            );
    }

    private async Task<bool> ProcessAsync(IHost host, CancellationToken? cancellation = null)
    {
        var handler = host
            .Services.GetRequiredService<InboxCatalog<TestDb>>()
            .Handlers.Single(h => h.Id == "locations");
        await using var scope = host.Services.CreateAsyncScope();
        return await scope
            .ServiceProvider.GetRequiredService<InboxProcessor<TestDb>>()
            .ProcessOneAsync(handler, "business-worker", cancellation ?? Token);
    }

    private static byte[] Event(long sequence, string key = "object-1") =>
        JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                event_id = $"object:created:{key}:{sequence}",
                producer = "service",
                sequence_scope = "objects",
                object_key = key,
                sequence,
                event_type = "object.created",
                occurred_at = "2026-10-08T01:00:00Z",
                payload = new { value = 42 },
            }
        );

    private Task<long> BusinessCountAsync() => ScalarAsync<long>("SELECT count(*) FROM public.processing_business");

    private Task<long> CountAsync(string table, string condition = "true") =>
        ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".{table} WHERE {condition}");

    private async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = new NpgsqlConnection(infrastructure.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(Token))!;
    }

    private async Task SqlAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(infrastructure.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Token);
    }

    private async Task UntilAsync(Func<Task<bool>> ready)
    {
        var timer = Stopwatch.StartNew();
        while (!await ready())
        {
            Assert.True(timer.Elapsed < TimeSpan.FromSeconds(10), "Timed out waiting for inbox processing.");
            await Task.Delay(20, Token);
        }
    }

    private sealed class Probe
    {
        internal readonly ConcurrentQueue<(string Handler, string Key, long Sequence)> Calls = new();
        internal readonly ConcurrentQueue<InboxDiagnostics> Scopes = new();
        internal Func<InboxDiagnostics, JsonElement, CancellationToken, Task> Diagnose { get; set; } =
            (_, _, _) => Task.CompletedTask;
        internal Func<TestDb, JsonElement, CancellationToken, Task<InboxResult>> Handle { get; set; } =
            (_, _, _) => Task.FromResult(InboxResult.Applied);
    }

    private sealed class Dependency(TestDb context, InboxDiagnostics diagnostics)
    {
        internal TestDb Context => context;
        internal InboxDiagnostics Diagnostics => diagnostics;
    }

    private class Handler(
        TestDb scopedDb,
        Dependency dependency,
        Probe probe,
        InboxDiagnostics diagnostics,
        string handler
    ) : IInboxHandler<TestDb>
    {
        public async Task<InboxResult> HandleAsync(TestDb db, JsonElement message, CancellationToken ct)
        {
            Assert.Same(db, scopedDb);
            Assert.Same(db, dependency.Context);
            Assert.Same(diagnostics, dependency.Diagnostics);
            Assert.NotNull(diagnostics.AttemptId);
            probe.Scopes.Enqueue(diagnostics);
            await probe.Diagnose(diagnostics, message, ct);
            Assert.NotNull(db.Database.CurrentTransaction);
            Assert.Equal(42, message.GetProperty("payload").GetProperty("value").GetInt32());
            var key = message.GetProperty("object_key").GetString()!;
            var sequence = message.GetProperty("sequence").GetInt64();
            probe.Calls.Enqueue((handler, key, sequence));
            db.Add(
                new BusinessWrite
                {
                    Id = Guid.NewGuid(),
                    Key = key,
                    Sequence = sequence,
                    Kind = "saved",
                }
            );
            await db.SaveChangesAsync(ct);
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                INSERT INTO public.processing_business ("Id", "Key", "Sequence", "Kind")
                VALUES ({Guid.NewGuid()}, {key}, {sequence}, 'raw')
                """,
                ct
            );
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"INSERT INTO public.processing_outbox VALUES ({Guid.NewGuid()})",
                ct
            );
            db.Add(
                new BusinessWrite
                {
                    Id = Guid.NewGuid(),
                    Key = key,
                    Sequence = sequence,
                    Kind = "tracked",
                }
            );
            return await probe.Handle(db, message, ct);
        }
    }

    private sealed class LocationsHandler(TestDb db, Dependency dependency, Probe probe, InboxDiagnostics diagnostics)
        : Handler(db, dependency, probe, diagnostics, "locations");

    private sealed class ProductsHandler(TestDb db, Dependency dependency, Probe probe, InboxDiagnostics diagnostics)
        : Handler(db, dependency, probe, diagnostics, "products");

    private sealed class TestDb(DbContextOptions<TestDb> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<BusinessWrite>().ToTable("processing_business", "public");
    }

    private sealed class BusinessWrite
    {
        public Guid Id { get; set; }
        public string Key { get; set; } = "";
        public long Sequence { get; set; }
        public string Kind { get; set; } = "";
    }

    private sealed record LogRecord(string Category, Dictionary<string, object?> Fields);

    private sealed class LogCapture : ILoggerProvider, ISupportExternalScope
    {
        internal readonly ConcurrentQueue<LogRecord> Records = new();
        private IExternalScopeProvider _scopes = new LoggerExternalScopeProvider();

        public void SetScopeProvider(IExternalScopeProvider scopeProvider) => _scopes = scopeProvider;

        public ILogger CreateLogger(string categoryName) => new CaptureLogger(this, categoryName);

        public void Dispose() { }

        private sealed class CaptureLogger(LogCapture owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => owner._scopes.Push(state);

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel level,
                EventId eventId,
                TState state,
                Exception? error,
                Func<TState, Exception?, string> formatter
            )
            {
                var fields = new Dictionary<string, object?>();
                owner._scopes.ForEachScope(
                    (scope, target) =>
                    {
                        if (scope is IEnumerable<KeyValuePair<string, object?>> values)
                            foreach (var value in values)
                                target[value.Key] = value.Value;
                    },
                    fields
                );
                if (state is IEnumerable<KeyValuePair<string, object?>> values)
                    foreach (var value in values)
                        fields[value.Key] = value.Value;
                owner.Records.Enqueue(new(category, fields));
            }
        }
    }

    private sealed class CommitFailure(bool afterCommit) : DbTransactionInterceptor
    {
        internal bool Armed { get; set; }

        public override ValueTask<InterceptionResult> TransactionCommittingAsync(
            DbTransaction transaction,
            TransactionEventData eventData,
            InterceptionResult result,
            CancellationToken ct = default
        )
        {
            if (Armed && !afterCommit)
            {
                Armed = false;
                throw new IOException("Simulated loss before COMMIT reaches PostgreSQL.");
            }
            return ValueTask.FromResult(result);
        }

        public override Task TransactionCommittedAsync(
            DbTransaction transaction,
            TransactionEndEventData eventData,
            CancellationToken ct = default
        )
        {
            if (Armed && afterCommit)
            {
                Armed = false;
                throw new IOException("Simulated lost COMMIT acknowledgement after PostgreSQL committed.");
            }
            return Task.CompletedTask;
        }
    }
}

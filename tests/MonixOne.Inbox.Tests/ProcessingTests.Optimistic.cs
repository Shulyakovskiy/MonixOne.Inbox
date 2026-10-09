using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Tests;

public sealed partial class ProcessingTests
{
    [Fact]
    public async Task First_available_sequence_is_selected_after_persisted_startup_window_even_after_restart()
    {
        var host = HostFor(configure: h => h.StartupReorderWindow = TimeSpan.FromHours(1));
        await SeedAsync(host, 157);
        Assert.False(await ProcessAsync(host));
        var restarted = HostFor(configure: h => h.StartupReorderWindow = TimeSpan.FromHours(1));
        Assert.False(await ProcessAsync(restarted));
        Assert.Empty(_probe.Calls);
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL AND version = 0"));
        await SqlAsync($"UPDATE \"{_schema}\".inbox SET received_at = now() - interval '2 hours'");
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(157, Assert.Single(_probe.Calls).Sequence);
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157 AND version = 1"));
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'initial_sequence_selected'"));
        Assert.Equal(0, await CountAsync("inbox_journal", "decision = 'gap_skipped'"));
    }

    [Fact]
    public async Task Startup_reorders_157_155_156_and_selects_the_minimum_available_number()
    {
        var host = HostFor(configure: h => h.StartupReorderWindow = TimeSpan.FromHours(1));
        foreach (var sequence in new long[] { 157, 155, 156 })
            await SeedAsync(host, sequence);
        Assert.False(await ProcessAsync(host));
        await SqlAsync($"UPDATE \"{_schema}\".inbox SET received_at = now() - interval '2 hours'");
        for (var i = 0; i < 3; i++)
            Assert.True(await ProcessAsync(host));
        Assert.Equal(new long[] { 155, 156, 157 }, _probe.Calls.Select(x => x.Sequence));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157 AND version = 3"));
    }

    [Fact]
    public async Task Gap_waits_for_158_then_159_and_closes_160_only_after_the_persisted_deadline()
    {
        var host = HostFor(configure: h => h.GapTimeout = TimeSpan.FromHours(1));
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        await SeedAsync(host, 159);
        Assert.False(await ProcessAsync(host));
        await SeedAsync(host, 158);
        Assert.True(await ProcessAsync(host));
        Assert.True(await ProcessAsync(host));
        Assert.Equal(new long[] { 157, 158, 159 }, _probe.Calls.Select(x => x.Sequence));
        await SeedAsync(host, 162);
        Assert.False(await ProcessAsync(host));
        var restarted = HostFor(configure: h => h.GapTimeout = TimeSpan.FromHours(1));
        Assert.False(await ProcessAsync(restarted));
        await SqlAsync($"UPDATE \"{_schema}\".inbox SET received_at = now() - interval '2 hours' WHERE sequence = 162");
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'gap_skipped' AND " +
            "details ->> 'first_missing' = '160' AND details ->> 'last_missing' = '161'"));
        await SeedAsync(restarted, 160);
        var before = _probe.Calls.Count;
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(before, _probe.Calls.Count);
        Assert.Equal(1, await CountAsync("inbox", "sequence = 160 AND status = 'skipped' AND code = 'stale_sequence'"));
        await SeedAsync(restarted, 163);
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 163"));
    }

    [Fact]
    public async Task Retry_during_a_gap_does_not_close_the_range_until_the_terminal_decision()
    {
        var host = HostFor();
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        await SeedAsync(host, 160);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Retry("Wait", TimeSpan.FromHours(1)));
        Assert.True(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157 AND version = 2"));
        Assert.Equal(0, await CountAsync("inbox_journal", "decision = 'gap_skipped'"));
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Reject("Terminal"));
        await SqlAsync($"UPDATE \"{_schema}\".inbox SET next_attempt_at = now() - interval '1 second'");
        Assert.True(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 160 AND version = 3"));
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'gap_skipped' AND " +
            "details ->> 'first_missing' = '158' AND details ->> 'last_missing' = '159'"));
        await SeedAsync(host, 161);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        Assert.True(await ProcessAsync(host));
    }

    [Fact]
    public async Task Three_replicas_can_invoke_callback_but_only_one_commits_business_outbox_and_attempt()
    {
        var allEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrivals = 0;
        _probe.Handle = async (_, _, token) =>
        {
            if (Interlocked.Increment(ref arrivals) == 3)
                allEntered.TrySetResult();
            await allEntered.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
            return InboxResult.Applied;
        };
        var hosts = Enumerable.Range(0, 3).Select(_ => HostFor()).ToArray();
        await SeedAsync(hosts[0], 157);
        var results = await Task.WhenAll(hosts.Select(x => ProcessAsync(x)));
        Assert.Single(results, x => x);
        Assert.Equal(3, _probe.Calls.Count);
        Assert.Equal(3, await BusinessCountAsync());
        Assert.Equal(1, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(1, await CountAsync("inbox_attempts"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157 AND version = 1"));
        await SeedAsync(hosts[0], 158);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        Assert.Single(await Task.WhenAll(hosts.Select(x => ProcessAsync(x))), x => x);
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 158 AND version = 2"));
        Assert.Equal(6, await BusinessCountAsync());
    }

    [Fact]
    public async Task Earlier_event_visible_before_final_CAS_rolls_back_the_selected_later_event()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Handle = async (_, _, token) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
            return InboxResult.Applied;
        };
        var host = HostFor();
        await SeedAsync(host, 157);
        var processing = ProcessAsync(host);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Token);
        await SeedAsync(host, 155);
        await SeedAsync(host, 156);
        release.TrySetResult();
        Assert.False(await processing);
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await ScalarAsync<long>("SELECT count(*) FROM public.processing_outbox"));
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL AND version = 0"));
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        for (var i = 0; i < 3; i++)
            Assert.True(await ProcessAsync(host));
        Assert.Equal(new long[] { 155, 156, 157 }, _probe.Calls.Skip(1).Select(x => x.Sequence));
        Assert.Equal(9, await BusinessCountAsync());
    }

    [Fact]
    public async Task Busy_local_key_is_excluded_while_intake_and_another_key_complete_on_free_slots()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Handle = async (_, message, token) =>
        {
            if (message.GetProperty("object_key").GetString() == "object-1")
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(token);
            }
            return InboxResult.Applied;
        };
        var host = HostFor(dispatch: true, configure: h => h.MaxParallelObjects = 4);
        await host.StartAsync(Token);
        await _js.PublishAsync($"{_subject}.a", Event(157), cancellationToken: Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Token);
        try
        {
            // Same object intake does not acquire the coordinator row held by a business callback.
            await _js.PublishAsync($"{_subject}.a", Event(158), cancellationToken: Token);
            await _js.PublishAsync($"{_subject}.a", Event(157, "other"), cancellationToken: Token);
            await UntilAsync(async () => await CountAsync("inbox", "object_key = 'other' AND status = 'processed'") == 1);
            Assert.Equal(1, await CountAsync("inbox", "object_key = 'object-1' AND sequence = 158 AND status = 'pending'"));
            Assert.Single(_probe.Calls, x => x.Key == "object-1");
        }
        finally { release.TrySetResult(); }
        await UntilAsync(async () => await CountAsync("inbox", "status = 'processed'") == 3);
        Assert.Equal(9, await BusinessCountAsync());
    }

    [Fact]
    public async Task Coordinator_lock_timeout_rolls_back_saved_business_without_spending_attempt_budget()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Handle = async (_, _, token) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
            return InboxResult.Applied;
        };
        var host = HostFor();
        await SeedAsync(host, 157);
        var processing = ProcessAsync(host);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Token);
        await using var connection = new NpgsqlConnection(infrastructure.ConnectionString);
        await connection.OpenAsync(Token);
        await using var blocking = await connection.BeginTransactionAsync(Token);
        await using var command = new NpgsqlCommand($"UPDATE \"{_schema}\".consumer_state SET version = version", connection, blocking);
        await command.ExecuteNonQueryAsync(Token);
        release.TrySetResult();
        Assert.False(await processing.WaitAsync(TimeSpan.FromSeconds(5), Token));
        Assert.Equal(0, await BusinessCountAsync());
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        await blocking.RollbackAsync(Token);
        Assert.True(await ProcessAsync(host));
    }

    [Theory]
    [InlineData(33)]
    [InlineData(150)]
    [InlineData(1)]
    public async Task Oversized_or_deep_result_details_never_repeat_an_otherwise_valid_decision(int depth)
    {
        var raw = depth == 1 ? "{\"large\":\"" + new string('x', 100_000) + "\"}" :
            string.Concat(Enumerable.Repeat("{\"nested\":", depth)) + "42" + new string('}', depth);
        _probe.Handle = (_, _, _) =>
        {
            using var json = JsonDocument.Parse(raw, new JsonDocumentOptions { MaxDepth = 256 });
            return Task.FromResult(InboxResult.Ignore("No effect", details: json.RootElement));
        };
        var host = HostFor();
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        Assert.False(await ProcessAsync(host));
        Assert.Single(_probe.Calls);
        Assert.Equal(1, await CountAsync("inbox_attempts", "details ->> 'code' = 'diagnostics.invalid_or_oversized'"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157"));
    }

    [Fact]
    public async Task Cleanup_removes_expired_history_preserves_cursor_and_prevents_replay_after_restart()
    {
        var host = HostFor();
        await SeedAsync(host, 157);
        await SeedAsync(host, 158);
        Assert.True(await ProcessAsync(host));
        await AgeHistoryAsync();
        Assert.Equal(1, await CleanupAsync(host));
        Assert.Equal(1, await CountAsync("inbox", "sequence = 158 AND status = 'pending'"));
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157"));
        var restarted = HostFor();
        await SeedAsync(restarted, 157);
        Assert.True(await ProcessAsync(restarted));
        Assert.Single(_probe.Calls);
        Assert.True(await ProcessAsync(restarted));
        Assert.Equal(new long[] { 157, 158 }, _probe.Calls.Select(x => x.Sequence));
    }

    [Fact]
    public async Task Cleanup_retains_terminal_payload_and_history_until_dead_letter_retention_expires()
    {
        var host = HostFor();
        await SeedAsync(host, 157);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Reject("Invalid business state"));
        Assert.True(await ProcessAsync(host));
        await AgeHistoryAsync();
        Assert.Equal(0, await CleanupAsync(host));
        Assert.Equal(1, await CountAsync("inbox"));
        Assert.Equal(1, await CountAsync("inbox_attempts"));
        await SqlAsync($"UPDATE \"{_schema}\".inbox_dlq SET created_at = now() - interval '100 days'");
        Assert.True(await CleanupAsync(host) > 0);
        Assert.Equal(0, await CountAsync("inbox"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        Assert.Equal(0, await CountAsync("inbox_journal"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157"));
    }

    [Fact]
    public async Task Cleanup_removes_orphan_invalid_DLQ_and_conflicts_with_multiple_originals_safely()
    {
        var host = HostFor();
        await SaveRawAsync(host, [0xff]);
        await SeedAsync(host, 157);
        await SeedAsync(host, 157, "other");
        using var json = JsonDocument.Parse(Event(157, "other"));
        var incoming = json.RootElement.EnumerateObject().ToDictionary(x => x.Name, x => x.Value.Clone());
        incoming["event_id"] = JsonSerializer.SerializeToElement("object:created:object-1:157");
        await SaveRawAsync(host, JsonSerializer.SerializeToUtf8Bytes(incoming));
        await SqlAsync($"UPDATE \"{_schema}\".inbox_dlq SET created_at = now() - interval '100 days'");
        // Conflict history tied to active work must survive; unrelated invalid history can expire.
        Assert.Equal(1, await CleanupAsync(host));
        Assert.Equal(1, await CountAsync("inbox_dlq", "category = 'conflict'"));
        Assert.True(await ProcessAsync(host));
        Assert.True(await ProcessAsync(host));
        await AgeHistoryAsync();
        for (var i = 0; i < 3; i++)
            await CleanupAsync(host);
        Assert.Equal(0, await CountAsync("inbox"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        Assert.Equal(0, await CountAsync("inbox_journal"));
        Assert.Equal(2, await CountAsync("consumer_state", "last_sequence = 157"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Cleanup_is_idempotent_between_replicas_and_respects_disabled_configuration(bool enabled)
    {
        var host = HostFor(configureGlobal: o => o.HistoryCleanup.Enabled = enabled);
        var replica = HostFor(configureGlobal: o => o.HistoryCleanup.Enabled = enabled);
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        await AgeHistoryAsync();
        var results = await Task.WhenAll(CleanupAsync(host), CleanupAsync(replica));
        Assert.Equal(enabled ? 1 : 0, results.Sum());
        Assert.Equal(enabled ? 0 : 1, await CountAsync("inbox"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157"));
    }

    [Fact]
    public async Task Invalid_message_does_not_advance_cursor_and_the_next_number_uses_the_normal_gap_window()
    {
        var host = HostFor(configure: h => h.GapTimeout = TimeSpan.FromHours(1));
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        await SaveRawAsync(host, [0xff]);
        await SeedAsync(host, 159);
        Assert.False(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 157"));
        await SqlAsync($"UPDATE \"{_schema}\".inbox SET received_at = now() - interval '2 hours' WHERE sequence = 159");
        Assert.True(await ProcessAsync(host));
        Assert.Equal(new long[] { 157, 159 }, _probe.Calls.Select(x => x.Sequence));
        Assert.Equal(1, await CountAsync("inbox_dlq", "category = 'invalid_message'"));
        Assert.Equal(1, await CountAsync("inbox_journal", "decision = 'gap_skipped' AND details ->> 'first_missing' = '158' AND details ->> 'last_missing' = '158'"));
    }

    [Fact]
    public async Task Producers_share_an_object_key_but_keep_independent_cursors()
    {
        var host = HostFor();
        await SeedAsync(host, 157);
        using var document = JsonDocument.Parse(Event(155));
        var values = document.RootElement.EnumerateObject().ToDictionary(x => x.Name, x => x.Value.Clone());
        values["producer"] = JsonSerializer.SerializeToElement("another-service");
        values["event_id"] = JsonSerializer.SerializeToElement("another-service:object:created:155");
        await SaveRawAsync(host, JsonSerializer.SerializeToUtf8Bytes(values));
        Assert.True(await ProcessAsync(host));
        Assert.True(await ProcessAsync(host));
        Assert.Equal(1, await CountAsync("consumer_state", "producer = 'service' AND last_sequence = 157"));
        Assert.Equal(1, await CountAsync("consumer_state", "producer = 'another-service' AND last_sequence = 155"));
        Assert.Equal(6, await BusinessCountAsync());
    }

    [Fact]
    public async Task Terminal_DLQ_references_the_original_payload_and_retains_it_in_the_history_page()
    {
        var host = HostFor();
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Reject("Broken dependency"));
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        var id = await ScalarAsync<Guid>($"SELECT id FROM \"{_schema}\".inbox_dlq");
        var page = (await HistoryAsync(host, id))!;
        Assert.Equal(Event(157), Convert.FromBase64String(page.Inbox!.Value.GetProperty("raw_payload").GetString()!));
        Assert.Equal(1, await CountAsync("inbox_dlq", "envelope IS NULL AND octet_length(raw_payload) = 0 AND source = '{}'::jsonb"));
        Assert.Equal("dead", page.Inbox.Value.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Background_cleanup_failure_does_not_stop_intake_or_business_processing()
    {
        var logs = new LogCapture();
        var host = HostFor(logs: logs, configureGlobal: o => o.HistoryCleanup.Interval = TimeSpan.FromMilliseconds(20));
        await SeedAsync(host, 157);
        Assert.True(await ProcessAsync(host));
        await AgeHistoryAsync();
        await SqlAsync($"""
            CREATE FUNCTION "{_schema}".fail_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'Temporary database failure during cleanup'; END $$;
            CREATE TRIGGER fail_cleanup BEFORE DELETE ON "{_schema}".inbox
            FOR EACH ROW EXECUTE FUNCTION "{_schema}".fail_cleanup();
            """);
        await host.StartAsync(Token);
        await UntilAsync(() => Task.FromResult(logs.Records.Any(x => x.Category.Contains("InboxHistoryCleanup", StringComparison.Ordinal))));
        await _js.PublishAsync($"{_subject}.a", Event(158), cancellationToken: Token);
        await UntilAsync(async () => await CountAsync("inbox", "sequence = 158") == 1);
        Assert.True(await ProcessAsync(host));
        Assert.False(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopped.IsCancellationRequested);
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence = 158"));
        await SqlAsync($"DROP TRIGGER fail_cleanup ON \"{_schema}\".inbox");
        await UntilAsync(async () => await CountAsync("inbox", "sequence = 157") == 0);
    }

    private Task AgeHistoryAsync() => SqlAsync($"UPDATE \"{_schema}\".inbox SET completed_at = now() - interval '100 days' WHERE completed_at IS NOT NULL");

    private Task<int> CleanupAsync(IHost host) => new InboxHistoryCleanup<TestDb>(
        host.Services.GetRequiredService<IServiceScopeFactory>(), host.Services.GetRequiredService<InboxCatalog<TestDb>>(),
        host.Services.GetRequiredService<InboxStartupBarrier>(), NullLogger<InboxHistoryCleanup<TestDb>>.Instance)
        .CleanupBatchAsync(Token);
}

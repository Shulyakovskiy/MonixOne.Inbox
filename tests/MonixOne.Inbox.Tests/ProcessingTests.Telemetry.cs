using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MonixOne.Inbox.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Tests;

public sealed partial class ProcessingTests
{
    [Fact]
    public async Task Metrics_report_committed_receipts_outcomes_duration_and_delay_without_unbounded_labels()
    {
        using var capture = new MetricCapture(_schema);
        var host = HostFor();
        await SeedAsync(host, 1);
        await SeedAsync(host, 1);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Retry("Temporary", TimeSpan.FromMilliseconds(1)));
        Assert.True(await ProcessAsync(host));
        await Task.Delay(5, Token);
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Applied);
        Assert.True(await ProcessAsync(host));
        await SeedAsync(host, 1, "ignored");
        _probe.Handle = (_, _, _) => Task.FromResult(InboxResult.Ignore("Unsupported type"));
        Assert.True(await ProcessAsync(host));
        foreach (var decision in new[] { "duplicate", "retry", "applied", "ignored" })
            Assert.Single(
                capture.Records,
                m => m.Name == "monixone.inbox.decisions" && m.Tags["decision"] is string d && d == decision
            );
        Assert.Equal(3, capture.Records.Count(m => m.Name == "monixone.inbox.processing.duration"));
        Assert.Equal(3, capture.Records.Count(m => m.Name == "monixone.inbox.processing.delay"));
        Assert.All(
            capture.Records,
            m =>
            {
                Assert.True(m.Value >= 0);
                Assert.Equal("locations", m.Tags["handler_id"]);
                Assert.Equal("events", m.Tags["subscription_id"]);
                Assert.DoesNotContain("object_key", m.Tags.Keys);
                Assert.DoesNotContain("event_id", m.Tags.Keys);
                Assert.DoesNotContain("code", m.Tags.Keys);
            }
        );
    }

    [Fact]
    public async Task Rolled_back_business_decision_produces_no_success_or_duration_measurement()
    {
        using var capture = new MetricCapture(_schema);
        var host = HostFor();
        await SeedAsync(host, 1);
        await SqlAsync(
            $"""
            CREATE FUNCTION "{_schema}".fail_metrics_test() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.decision = 'processed' THEN RAISE EXCEPTION 'journal unavailable'; END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER fail_metrics_test BEFORE INSERT ON "{_schema}".inbox_journal
            FOR EACH ROW EXECUTE FUNCTION "{_schema}".fail_metrics_test();
            """
        );
        await Assert.ThrowsAsync<PostgresException>(() => ProcessAsync(host));
        Assert.DoesNotContain(capture.Records, m => m.Name == "monixone.inbox.processing.duration");
        Assert.DoesNotContain(capture.Records, m => m.Tags.GetValueOrDefault("decision") is "applied");
        Assert.Equal(0, await BusinessCountAsync());
        await SqlAsync($"DROP TRIGGER fail_metrics_test ON \"{_schema}\".inbox_journal");
        Assert.True(await ProcessAsync(host));
        Assert.Single(capture.Records, m => m.Tags.GetValueOrDefault("decision") is "applied");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Metrics_follow_verified_commit_outcome_without_duplicate_success(bool committed)
    {
        using var capture = new MetricCapture(_schema);
        var failure = new CommitFailure(committed);
        var host = HostFor(interceptor: failure);
        await SeedAsync(host, 1);
        failure.Armed = true;
        Assert.True(await ProcessAsync(host));
        Assert.Equal(committed ? 1 : 0, capture.Records.Count(m => m.Tags.GetValueOrDefault("decision") is "applied"));
        Assert.Equal(!committed, await ProcessAsync(host));
        Assert.Single(capture.Records, m => m.Tags.GetValueOrDefault("decision") is "applied");
        Assert.Single(capture.Records, m => m.Name == "monixone.inbox.processing.duration");
    }

    [Fact]
    public async Task Active_queue_gauge_reads_a_snapshot_and_preserves_it_when_database_access_fails()
    {
        using var capture = new MetricCapture(_schema);
        var host = HostFor();
        capture.Observe();
        Assert.DoesNotContain(capture.Records, x => x.Name == "monixone.inbox.active_messages");
        await SeedAsync(host, 157);
        await SeedAsync(host, 158);
        var reporter = new InboxMetricsReporter<TestDb>(host.Services.GetRequiredService<IServiceScopeFactory>(),
            host.Services.GetRequiredService<InboxCatalog<TestDb>>(), host.Services.GetRequiredService<InboxStartupBarrier>(),
            host.Services.GetRequiredService<InboxMetrics>(), NullLogger<InboxMetricsReporter<TestDb>>.Instance);
        await reporter.RefreshAsync(Token);
        capture.Observe();
        Assert.Equal(2, capture.Last("monixone.inbox.active_messages").Value);
        await SqlAsync($"ALTER TABLE \"{_schema}\".inbox RENAME TO inbox_temporarily_unavailable");
        try
        {
            await Assert.ThrowsAsync<PostgresException>(() => reporter.RefreshAsync(Token));
            capture.Observe();
            Assert.Equal(2, capture.Last("monixone.inbox.active_messages").Value);
            Assert.True(capture.Last("monixone.inbox.state_snapshot.age").Value > 0);
        }
        finally { await SqlAsync($"ALTER TABLE \"{_schema}\".inbox_temporarily_unavailable RENAME TO inbox"); }
    }

    private sealed record MetricRecord(string Name, double Value, Dictionary<string, object?> Tags);

    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener _listener = new();
        internal ConcurrentQueue<MetricRecord> Records { get; } = new();

        internal MetricCapture(string schema)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == InboxTelemetry.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((i, v, tags, _) => Capture(i, v, tags, schema));
            _listener.SetMeasurementEventCallback<double>((i, v, tags, _) => Capture(i, v, tags, schema));
            _listener.Start();
        }

        internal void Observe() => _listener.RecordObservableInstruments();

        internal MetricRecord Last(string name) => Records.Last(m => m.Name == name);

        private void Capture(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            string schema
        )
        {
            var fields = new Dictionary<string, object?>();
            foreach (var tag in tags)
                fields[tag.Key] = tag.Value;
            if (fields.GetValueOrDefault("schema") is string s && s == schema)
                Records.Enqueue(new(instrument.Name, value, fields));
        }

        public void Dispose() => _listener.Dispose();
    }
}

using System.Diagnostics;
using System.Diagnostics.Metrics;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Processing;

namespace MonixOne.Inbox;

/// <summary>
/// Имя Meter для подключения OpenTelemetry или другого System.Diagnostics.Metrics listener.
/// Exporter, endpoint и период экспорта настраиваются приложением.
/// </summary>
public static class InboxTelemetry
{
    /// <summary>
    /// Meter пакета: MonixOne.Inbox.
    /// Labels содержат только schema, handler, subscription и ограниченные виды решений.
    /// </summary>
    public const string MeterName = "MonixOne.Inbox";
}

internal sealed record InboxStateMeasurement(string HandlerId, long Active);

internal sealed record InboxMetricsSnapshot(string Schema, DateTimeOffset UpdatedAt, InboxStateMeasurement[] States);

internal sealed class InboxMetrics : IDisposable
{
    private readonly Meter _meter = new(InboxTelemetry.MeterName);
    private readonly Counter<long> _decisions;
    private readonly Histogram<double> _duration;
    private readonly Histogram<double> _delay;
    private InboxMetricsSnapshot? _snapshot;

    public InboxMetrics()
    {
        _decisions = _meter.CreateCounter<long>("monixone.inbox.decisions", "{decision}");
        _duration = _meter.CreateHistogram<double>("monixone.inbox.processing.duration", "s");
        _delay = _meter.CreateHistogram<double>("monixone.inbox.processing.delay", "s");
        _meter.CreateObservableGauge("monixone.inbox.active_messages", ObserveStates, "{message}");
        _meter.CreateObservableGauge("monixone.inbox.state_snapshot.age", ObserveAge, "s");
    }

    internal void Decision(string schema, string handler, string subscription, string decision)
    {
        var tags = new TagList
        {
            { "schema", schema },
            { "handler_id", handler },
            { "subscription_id", subscription },
            { "decision", decision },
        };
        _decisions.Add(1, tags);
    }

    internal void Processing(string schema, InboxRow row, InboxAttemptRow? attempt, string? committedStatus = null)
    {
        var decision = (committedStatus ?? row.Status) switch
        {
            "processed" => "applied",
            "skipped" when attempt is null => "stale",
            "skipped" => "ignored",
            var status => status,
        };
        Decision(schema, row.HandlerId, row.SubscriptionId, decision);
        if (attempt is null)
            return;
        var tags = new TagList
        {
            { "schema", schema },
            { "handler_id", row.HandlerId },
            { "subscription_id", row.SubscriptionId },
        };
        _duration.Record((attempt.DurationMs ?? 0) / 1000d, tags);
        _delay.Record(Math.Max(0, (attempt.StartedAt - row.ReceivedAt).TotalSeconds), tags);
    }

    internal void UpdateSnapshot(InboxMetricsSnapshot snapshot) => Volatile.Write(ref _snapshot, snapshot);

    private IEnumerable<Measurement<long>> ObserveStates()
    {
        var snapshot = Volatile.Read(ref _snapshot);
        if (snapshot is null)
            yield break;
        foreach (var state in snapshot.States)
            yield return new(
                state.Active,
                new KeyValuePair<string, object?>("schema", snapshot.Schema),
                new KeyValuePair<string, object?>("handler_id", state.HandlerId)
            );
    }

    private IEnumerable<Measurement<double>> ObserveAge()
    {
        if (Volatile.Read(ref _snapshot) is { } snapshot)
            yield return new(
                Math.Max(0, (DateTimeOffset.UtcNow - snapshot.UpdatedAt).TotalSeconds),
                new KeyValuePair<string, object?>("schema", snapshot.Schema)
            );
    }

    public void Dispose() => _meter.Dispose();
}

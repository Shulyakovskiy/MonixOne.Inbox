using LinqToDB.Mapping;

namespace MonixOne.Inbox.Processing;

[Table("inbox_attempts")]
internal sealed class InboxAttemptRow
{
    [Column("attempt_id"), PrimaryKey]
    public Guid AttemptId { get; set; }

    [Column("inbox_id")]
    public Guid InboxId { get; set; }

    [Column("run_id")]
    public Guid RunId { get; set; }

    [Column("attempt_number")]
    public int AttemptNumber { get; set; }

    [Column("worker_instance_id"), NotNull]
    public string WorkerInstanceId { get; set; } = "";

    [Column("handler_type"), NotNull]
    public string HandlerType { get; set; } = "";

    [Column("started_at")]
    public DateTimeOffset StartedAt { get; set; }

    [Column("finished_at")]
    public DateTimeOffset? FinishedAt { get; set; }

    [Column("duration_ms")]
    public long? DurationMs { get; set; }

    [Column("outcome"), NotNull]
    public string Outcome { get; set; } = "started";

    [Column("code")]
    public string? Code { get; set; }

    [Column("reason")]
    public string? Reason { get; set; }

    [Column("details", DataType = LinqToDB.DataType.BinaryJson)]
    public string? Details { get; set; }

    [Column("exception")]
    public string? Exception { get; set; }

    [Column("next_attempt_at")]
    public DateTimeOffset? NextAttemptAt { get; set; }

    [Column("correlation_id")]
    public string? CorrelationId { get; set; }

    [Column("trace_id")]
    public string? TraceId { get; set; }

    [Column("diagnostics", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Diagnostics { get; set; } = "[]";

    [Column("settings", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Settings { get; set; } = "{}";
}

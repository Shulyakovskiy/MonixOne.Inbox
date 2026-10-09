using LinqToDB.Mapping;

namespace MonixOne.Inbox.Intake;

[Table("consumer_state")]
internal class InboxStateRow
{
    [Column("handler_id"), PrimaryKey(0), NotNull]
    public string HandlerId { get; set; } = "";

    [Column("producer"), PrimaryKey(1), NotNull]
    public string Producer { get; set; } = "";

    [Column("sequence_scope"), PrimaryKey(2), NotNull]
    public string SequenceScope { get; set; } = "";

    [Column("object_key"), PrimaryKey(3), NotNull]
    public string ObjectKey { get; set; } = "";

    [Column("last_sequence")]
    public long? LastSequence { get; set; }

    [Column("version")]
    public long Version { get; set; }

    [Column("updated_at", SkipOnInsert = true)]
    public DateTimeOffset UpdatedAt { get; set; }
}

[Table("inbox")]
internal sealed class InboxRow
{
    [Column("id"), PrimaryKey]
    public Guid Id { get; set; }

    [Column("handler_id"), NotNull]
    public string HandlerId { get; set; } = "";

    [Column("subscription_id"), NotNull]
    public string SubscriptionId { get; set; } = "";

    [Column("event_id"), NotNull]
    public string EventId { get; set; } = "";

    [Column("producer"), NotNull]
    public string Producer { get; set; } = "";

    [Column("sequence_scope"), NotNull]
    public string SequenceScope { get; set; } = "";

    [Column("object_key"), NotNull]
    public string ObjectKey { get; set; } = "";

    [Column("sequence")]
    public long Sequence { get; set; }

    [Column("event_type"), NotNull]
    public string EventType { get; set; } = "";

    [Column("occurred_at")]
    public DateTimeOffset OccurredAt { get; set; }

    [Column("envelope", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Envelope { get; set; } = "";

    [Column("raw_payload"), NotNull]
    public byte[] RawPayload { get; set; } = [];

    [Column("source", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Source { get; set; } = "{}";

    [Column("run_id")]
    public Guid RunId { get; set; }

    [Column("next_attempt_at")]
    public DateTimeOffset? NextAttemptAt { get; set; }

    [Column("status"), NotNull]
    public string Status { get; set; } = "pending";

    [Column("attempt_count")]
    public int AttemptCount { get; set; }

    [Column("current_attempt_id")]
    public Guid? CurrentAttemptId { get; set; }

    [Column("completed_at")]
    public DateTimeOffset? CompletedAt { get; set; }

    [Column("code")]
    public string? Code { get; set; }

    [Column("reason")]
    public string? Reason { get; set; }

    [Column("received_at", SkipOnInsert = true)]
    public DateTimeOffset ReceivedAt { get; set; }

    [Column("updated_at", SkipOnInsert = true)]
    public DateTimeOffset UpdatedAt { get; set; }
}

[Table("inbox_dlq")]
internal sealed class InboxDlqRow
{
    [Column("id"), PrimaryKey]
    public Guid Id { get; set; }

    [Column("inbox_id")]
    public Guid? InboxId { get; set; }

    [Column("run_id")]
    public Guid? RunId { get; set; }

    [Column("dlq_policy")]
    public string? DlqPolicy { get; set; }

    [Column("cursor_advanced")]
    public bool CursorAdvanced { get; set; }

    [Column("exception")]
    public string? Exception { get; set; }

    [Column("related_inbox_id")]
    public Guid? RelatedInboxId { get; set; }

    [Column("handler_id"), NotNull]
    public string HandlerId { get; set; } = "";

    [Column("subscription_id"), NotNull]
    public string SubscriptionId { get; set; } = "";

    [Column("deduplication_key"), NotNull]
    public string DeduplicationKey { get; set; } = "";

    [Column("category"), NotNull]
    public string Category { get; set; } = "";

    [Column("event_id")]
    public string? EventId { get; set; }

    [Column("producer")]
    public string? Producer { get; set; }

    [Column("sequence_scope")]
    public string? SequenceScope { get; set; }

    [Column("object_key")]
    public string? ObjectKey { get; set; }

    [Column("sequence")]
    public long? Sequence { get; set; }

    [Column("event_type")]
    public string? EventType { get; set; }

    [Column("envelope", DataType = LinqToDB.DataType.BinaryJson)]
    public string? Envelope { get; set; }

    [Column("raw_payload"), NotNull]
    public byte[] RawPayload { get; set; } = [];

    [Column("source", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Source { get; set; } = "{}";

    [Column("code"), NotNull]
    public string Code { get; set; } = "";

    [Column("reason"), NotNull]
    public string Reason { get; set; } = "";

    [Column("last_sequence")]
    public long? LastSequence { get; set; }

    [Column("worker_instance_id"), NotNull]
    public string WorkerInstanceId { get; set; } = "";

    [Column("handler_type"), NotNull]
    public string HandlerType { get; set; } = "";

    [Column("package_version"), NotNull]
    public string PackageVersion { get; set; } = "";

    [Column("settings", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Settings { get; set; } = "{}";

    [Column("created_at", SkipOnInsert = true)]
    public DateTimeOffset CreatedAt { get; set; }
}

[Table("inbox_journal")]
internal sealed class InboxJournalRow
{
    [Column("id"), PrimaryKey, Identity]
    public long Id { get; set; }

    [Column("inbox_id")]
    public Guid? InboxId { get; set; }

    [Column("dlq_id")]
    public Guid? DlqId { get; set; }

    [Column("attempt_id")]
    public Guid? AttemptId { get; set; }

    [Column("decision_id")]
    public Guid? DecisionId { get; set; }

    [Column("created_at", SkipOnInsert = true)]
    public DateTimeOffset CreatedAt { get; set; }

    [Column("handler_id"), NotNull]
    public string HandlerId { get; set; } = "";

    [Column("subscription_id")]
    public string? SubscriptionId { get; set; }

    [Column("worker_instance_id"), NotNull]
    public string WorkerInstanceId { get; set; } = "";

    [Column("decision"), NotNull]
    public string Decision { get; set; } = "";

    [Column("details", DataType = LinqToDB.DataType.BinaryJson), NotNull]
    public string Details { get; set; } = "{}";
}

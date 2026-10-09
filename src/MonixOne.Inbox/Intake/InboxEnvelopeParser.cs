using System.Text;
using System.Text.Json;

namespace MonixOne.Inbox.Intake;

internal sealed record ParsedInboxEvent(string? Json, InboxEventMetadata? Metadata, string? Error);

internal static class InboxEnvelopeParser
{
    internal static ParsedInboxEvent Parse(byte[] raw, IInboxEnvelopeAdapter? adapter = null)
    {
        string? json = null;
        try
        {
            using var document = JsonDocument.Parse(raw);
            json = document.RootElement.GetRawText();
            var root = document.RootElement;
            var metadata = adapter is not null
                ? adapter.Extract(root)
                : new InboxEventMetadata(
                    Text(root, "event_id"),
                    Text(root, "producer"),
                    Text(root, "sequence_scope"),
                    Text(root, "object_key"),
                    At(root, "sequence").GetInt64(),
                    Text(root, "event_type"),
                    OccurredAt(root, "occurred_at")
                );
            if (
                !ValidText(metadata.EventId, 512)
                || !ValidText(metadata.Producer, 256)
                || !ValidText(metadata.SequenceScope, 256)
                || !ValidText(metadata.ObjectKey, 1024)
                || !ValidText(metadata.EventType, int.MaxValue)
                || metadata.Sequence <= 0
            )
                return new(
                    json,
                    null,
                    "Required identity fields must be nonempty, within UTF-8 byte limits; sequence "
                        + "must be a positive Int64."
                );

            var utc = metadata.OccurredAt.ToUniversalTime();
            // PostgreSQL timestamptz stores microseconds; compare using the same precision.
            return new(json, metadata with { OccurredAt = new(utc.Ticks - utc.Ticks % 10, TimeSpan.Zero) }, null);
        }
        catch (Exception error)
        {
            // Never interpolate payload or callback exception text into logs/diagnostics.
            return new(json, null, $"Envelope extraction failed ({error.GetType().Name}).");
        }
    }

    internal static bool ValidText(string? value, int bytes) =>
        !string.IsNullOrWhiteSpace(value) && !value.Contains('\0') && Encoding.UTF8.GetByteCount(value) <= bytes;

    private static DateTimeOffset OccurredAt(JsonElement root, string path)
    {
        var value = At(root, path);
        var text = value.GetString() ?? throw new FormatException("Expected an ISO timestamp.");
        var time = text.IndexOf('T');
        if (time < 0 || !(text.EndsWith('Z') || text.AsSpan(time).Contains('+') || text.AsSpan(time).Contains('-')))
            throw new FormatException("Timestamp must have an explicit UTC offset.");
        return value.GetDateTimeOffset();
    }

    private static string Text(JsonElement root, string path) =>
        At(root, path).GetString() ?? throw new FormatException("Expected a string.");

    private static JsonElement At(JsonElement root, string name) => root.GetProperty(name);
}

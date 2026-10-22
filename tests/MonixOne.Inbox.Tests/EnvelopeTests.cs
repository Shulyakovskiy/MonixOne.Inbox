using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using MonixOne.Inbox.Intake;
using Xunit;

namespace MonixOne.Inbox.Tests;

public sealed class EnvelopeTests
{
    private const string DocumentationEnvelope = """
        {
          "eventId": "profile:updated:42:157",
          "producer": "profiles",
          "sequenceScope": "profiles",
          "objectKey": "42",
          "sequence": 157,
          "eventType": "profile.updated",
          "occurredAt": "2026-10-09T07:00:00Z",
          "payload": { "name": "Example" }
        }
        """;

    [Fact]
    public void Documentation_envelope_is_valid_and_preserves_the_complete_message()
    {
        var parsed = InboxEnvelopeParser.Parse(Encoding.UTF8.GetBytes(DocumentationEnvelope));

        Assert.Null(parsed.Error);
        Assert.Equal(
            new InboxEventMetadata("profile:updated:42:157", "profiles", "profiles", "42", 157,
                "profile.updated", new DateTimeOffset(2026, 10, 9, 7, 0, 0, TimeSpan.Zero)),
            parsed.Metadata);
        Assert.Equal(DocumentationEnvelope, parsed.Json);
    }

    [Fact]
    public void Metadata_serialized_with_default_options_is_a_valid_camel_case_envelope()
    {
        var metadata = new InboxEventMetadata("profile:updated:42:157", "profiles", "profiles", "42", 157,
            "profile.updated", new DateTimeOffset(2026, 10, 9, 7, 0, 0, TimeSpan.Zero));
        var raw = JsonSerializer.SerializeToUtf8Bytes(metadata);

        var parsed = InboxEnvelopeParser.Parse(raw);

        Assert.Null(parsed.Error);
        Assert.Equal(metadata, parsed.Metadata);
        Assert.Equal(metadata, JsonSerializer.Deserialize<InboxEventMetadata>(raw));
    }

    [Theory]
    [InlineData("\"sequence\": 157", "\"sequence\": \"157\"")]
    [InlineData("\"objectKey\": \"42\"", "\"objectKey\": 42")]
    [InlineData("\"eventId\"", "\"event_id\"")]
    [InlineData("\"eventId\"", "\"EventId\"")]
    [InlineData("\"sequenceScope\"", "\"sequence_scope\"")]
    [InlineData("\"objectKey\"", "\"object_key\"")]
    [InlineData("\"eventType\"", "\"event_type\"")]
    [InlineData("\"occurredAt\"", "\"occurred_at\"")]
    [InlineData("2026-10-09T07:00:00Z", "2026-10-09T07:00:00")]
    public void Required_fields_use_exact_names_and_json_types(string original, string replacement)
    {
        var parsed = InboxEnvelopeParser.Parse(
            Encoding.UTF8.GetBytes(DocumentationEnvelope.Replace(original, replacement, StringComparison.Ordinal)));

        Assert.Null(parsed.Metadata);
        Assert.NotNull(parsed.Error);
    }

    [Fact]
    public void Json_encoded_string_is_not_an_envelope_object()
    {
        var parsed = InboxEnvelopeParser.Parse(JsonSerializer.SerializeToUtf8Bytes(DocumentationEnvelope));

        Assert.Null(parsed.Metadata);
        Assert.NotNull(parsed.Error);
    }

    [Fact]
    public void Transport_wrapper_does_not_expose_identity_fields_inside_data_to_the_default_parser()
    {
        var raw = JsonSerializer.SerializeToUtf8Bytes(new
        {
            EventId = "transport-event-id",
            Type = "profile.updated",
            Version = 1,
            OccurredAt = "2026-10-09T07:00:00Z",
            Data = DocumentationEnvelope,
        });

        var parsed = InboxEnvelopeParser.Parse(raw);

        Assert.Null(parsed.Metadata);
        Assert.Equal("Envelope extraction failed (KeyNotFoundException).", parsed.Error);
        Assert.NotNull(parsed.Json);
    }

    [Fact]
    public void Payload_is_optional_and_has_no_inbox_specific_schema()
    {
        var message = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(DocumentationEnvelope)!;
        message.Remove("payload");
        Assert.Null(InboxEnvelopeParser.Parse(JsonSerializer.SerializeToUtf8Bytes(message)).Error);

        message["payload"] = JsonSerializer.SerializeToElement(new[] { 1, 2, 3 });
        Assert.Null(InboxEnvelopeParser.Parse(JsonSerializer.SerializeToUtf8Bytes(message)).Error);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    public void Nonpositive_sequence_is_invalid(long sequence)
    {
        var raw = Encoding.UTF8.GetBytes(
            $$"""
            {"eventId":"created-1","producer":"p","sequenceScope":"s","objectKey":"k",
             "sequence":{{sequence}},"eventType":"object.created","occurredAt":"2026-10-08T01:00:00Z"}
            """
        );
        var parsed = InboxEnvelopeParser.Parse(raw, null);
        Assert.Null(parsed.Metadata);
        Assert.NotNull(parsed.Error);
        Assert.NotNull(parsed.Json);
    }

    [Fact]
    public void Utf8_byte_limits_and_nul_are_checked_before_database_write()
    {
        Assert.True(InboxEnvelopeParser.ValidText(new string('я', 128), 256));
        Assert.False(InboxEnvelopeParser.ValidText(new string('я', 129), 256));
        Assert.False(InboxEnvelopeParser.ValidText("object\0id", 1024));
        Assert.False(InboxEnvelopeParser.ValidText(" ", 1024));
    }
}

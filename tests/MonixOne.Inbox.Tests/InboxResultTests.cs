using System.Text.Json;

namespace MonixOne.Inbox.Tests;

public sealed class InboxResultTests
{
    [Fact]
    public void Result_serialization_uses_camel_case_and_preserves_custom_details()
    {
        using var details = JsonDocument.Parse("""{"profile_id":42}""");
        var delay = TimeSpan.FromSeconds(5);
        var result = InboxResult.Retry("Profile is not ready.", delay, details: details.RootElement);

        var json = JsonSerializer.SerializeToElement(result);

        Assert.Equal(new[] { "kind", "code", "reason", "retryAfter", "details" },
            json.EnumerateObject().Select(x => x.Name));
        Assert.Equal(delay, json.GetProperty("retryAfter").Deserialize<TimeSpan>());
        Assert.Equal(42, json.GetProperty("details").GetProperty("profile_id").GetInt32());
    }

    [Fact]
    public void Diagnostic_json_survives_the_callers_document_disposal()
    {
        InboxResult result;
        using (var document = JsonDocument.Parse("""{"dependency":"profile"}"""))
            result = InboxResult.Retry("Profile is not ready.", details: document.RootElement);

        Assert.Equal("profile", result.Details!.Value.GetProperty("dependency").GetString());
    }

    [Fact]
    public void Retry_delay_cannot_be_zero_or_negative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => InboxResult.Retry("Later", TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => InboxResult.Retry("Later", TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void Result_code_and_reason_reject_NUL_before_PostgreSql_persistence()
    {
        Assert.Throws<ArgumentException>(() => InboxResult.Retry("Invalid\0reason"));
        Assert.Throws<ArgumentException>(() => InboxResult.Reject("Invalid code", "invalid\0code"));
    }

    [Fact]
    public void Non_applied_results_require_a_reason()
    {
        Assert.Throws<ArgumentException>(() => InboxResult.Ignore(" "));
        Assert.Throws<ArgumentException>(() => InboxResult.Retry(" "));
        Assert.Throws<ArgumentException>(() => InboxResult.Reject(" "));
    }
}

using System.Text;
using System.Text.Json;

namespace MonixOne.Inbox;

internal static class InboxDiagnosticJson
{
    internal const int MaxDepth = 32;
    internal const int MaxBytes = 64 * 1024;
    private const string Fallback = "{\"code\":\"diagnostics.invalid_or_oversized\"}";

    internal static string? Bound(JsonElement? value)
    {
        if (value is null)
            return null;
        try
        {
            var json = value.Value.GetRawText();
            if (Encoding.UTF8.GetByteCount(json) > MaxBytes)
                return Fallback;
            using var document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = MaxDepth });
            return json;
        }
        catch (Exception error) when (error is JsonException or InvalidOperationException or ObjectDisposedException)
        {
            return Fallback;
        }
    }

    internal static JsonElement? Copy(JsonElement? value)
    {
        var json = Bound(value);
        if (json is null)
            return null;
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}

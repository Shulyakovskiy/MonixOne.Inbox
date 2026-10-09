using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MonixOne.Inbox.Tests;

public sealed class InboxDiagnosticsTests
{
    [Fact]
    public void Details_survive_document_disposal_and_disabled_ILogger()
    {
        using var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        var id = Guid.NewGuid();
        diagnostics.Begin(id);
        using (var details = JsonDocument.Parse("{\"profile_id\":42}"))
            diagnostics.Write("profile.loaded", "Profile found.", details.RootElement);
        Assert.Equal(id, diagnostics.AttemptId);
        using var snapshot = JsonDocument.Parse(diagnostics.Complete());
        var entry = Assert.Single(snapshot.RootElement.EnumerateArray());
        Assert.Equal(42, entry.GetProperty("details").GetProperty("profile_id").GetInt32());
        Assert.Equal("Information", entry.GetProperty("level").GetString());
        Assert.Null(diagnostics.AttemptId);
        Assert.Throws<InvalidOperationException>(() => diagnostics.Write("late", "No active attempt."));
    }

    [Fact]
    public void Full_envelope_at_standard_parser_depth_can_be_written_to_diagnostics()
    {
        using var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        diagnostics.Begin(Guid.NewGuid());
        var raw = string.Concat(Enumerable.Repeat("{\"nested\":", 64)) + "42" + new string('}', 64);
        using var document = JsonDocument.Parse(raw);
        diagnostics.Write("deep", "Keep full diagnostic details.", document.RootElement);
        using var snapshot = JsonDocument.Parse(diagnostics.Complete(), new JsonDocumentOptions { MaxDepth = 128 });
        Assert.Equal("diagnostics.invalid_or_oversized", snapshot.RootElement[0].GetProperty("details").GetProperty("code").GetString());
    }

    [Fact]
    public void Entry_and_byte_limits_are_explicit_and_do_not_throw()
    {
        using var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        diagnostics.Begin(Guid.NewGuid());
        for (var i = 0; i < 500; i++)
            diagnostics.Write($"step.{i}", new string('x', 1024));
        var json = diagnostics.Complete();
        Assert.True(Encoding.UTF8.GetByteCount(json) <= InboxDiagnostics.MaxBytes);
        using var snapshot = JsonDocument.Parse(json);
        var entries = snapshot.RootElement.EnumerateArray().ToArray();
        Assert.InRange(entries.Length, 2, InboxDiagnostics.MaxEntries);
        Assert.Equal("diagnostics.truncated", entries[^1].GetProperty("code").GetString());
        Assert.Equal(500L, entries.Length - 1 + entries[^1].GetProperty("dropped_entries").GetInt64());
    }

    [Fact]
    public void Single_oversized_record_is_replaced_with_truncation_marker()
    {
        using var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        diagnostics.Begin(Guid.NewGuid());
        diagnostics.Write("large", new string('x', InboxDiagnostics.MaxBytes * 2));
        using var snapshot = JsonDocument.Parse(diagnostics.Complete());
        var entry = Assert.Single(snapshot.RootElement.EnumerateArray());
        Assert.Equal("diagnostics.truncated", entry.GetProperty("code").GetString());
        Assert.Equal(1, entry.GetProperty("dropped_entries").GetInt32());
    }

    [Fact]
    public async Task Awaited_parallel_diagnostics_are_collected_without_corrupting_the_attempt()
    {
        using var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        diagnostics.Begin(Guid.NewGuid());
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(i =>
                    Task.Run(
                        () => diagnostics.Write($"step.{i}", "Parallel work completed."),
                        TestContext.Current.CancellationToken
                    )
                )
        );
        using var snapshot = JsonDocument.Parse(diagnostics.Complete());
        var codes = snapshot.RootElement.EnumerateArray().Select(x => x.GetProperty("code").GetString()).ToArray();
        Assert.Equal(32, codes.Distinct().Count());
    }

    [Fact]
    public void Writes_outside_attempt_after_scope_disposal_and_invalid_levels_are_rejected()
    {
        var diagnostics = new InboxDiagnostics(NullLogger<InboxDiagnostics>.Instance);
        Assert.Throws<InvalidOperationException>(() => diagnostics.Write("outside", "No attempt."));
        diagnostics.Begin(Guid.NewGuid());
        Assert.Throws<ArgumentOutOfRangeException>(() => diagnostics.Write("none", "Message", level: LogLevel.None));
        Assert.Throws<ArgumentException>(() => diagnostics.Write(" ", "Message"));
        diagnostics.Dispose();
        Assert.Null(diagnostics.AttemptId);
        Assert.Throws<ObjectDisposedException>(() => diagnostics.Write("disposed", "Scope ended."));
    }
}

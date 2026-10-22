using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Tests;

public sealed partial class ProcessingTests
{
    [Fact]
    public async Task Serialized_history_uses_camel_case_and_preserves_custom_json_and_headers()
    {
        _probe.Diagnose = (diagnostics, _, _) =>
        {
            using var details = JsonDocument.Parse("""{"profile_id":42}""");
            diagnostics.Write("profile.checked", "Profile checked.", details.RootElement);
            return Task.CompletedTask;
        };
        _probe.Handle = (_, _, _) =>
        {
            using var details = JsonDocument.Parse("""{"profile_id":42}""");
            return Task.FromResult(InboxResult.Reject("Unsupported profile.", details: details.RootElement));
        };
        var host = HostFor();
        await SeedAsync(host, 1);
        Assert.True(await ProcessAsync(host));
        var id = await ScalarAsync<Guid>($"SELECT id FROM \"{_schema}\".inbox_dlq");

        var page = (await HistoryAsync(host, id))!;
        var json = JsonSerializer.SerializeToElement(page);

        Assert.Equal(new[] { "inbox", "consumerState", "deadLetter", "entries", "nextJournalId" },
            json.EnumerateObject().Select(x => x.Name));
        Assert.Equal("object:created:object-1:1", json.GetProperty("inbox").GetProperty("eventId").GetString());
        Assert.Equal(Event(1), json.GetProperty("inbox").GetProperty("rawPayload").GetBytesFromBase64());
        Assert.Equal("correlation-1", json.GetProperty("inbox").GetProperty("source")
            .GetProperty("headers").GetProperty("x-correlation-id")[0].GetString());
        Assert.Equal(1, json.GetProperty("consumerState").GetProperty("lastSequence").GetInt64());
        Assert.Equal(id, json.GetProperty("deadLetter").GetProperty("id").GetGuid());
        var entry = Assert.Single(json.GetProperty("entries").EnumerateArray(), x =>
            x.GetProperty("attempt").ValueKind == JsonValueKind.Object);
        Assert.Equal(new[] { "journalId", "journal", "attempt", "deadLetter" },
            entry.EnumerateObject().Select(x => x.Name));
        Assert.Equal("rejected", entry.GetProperty("attempt").GetProperty("outcome").GetString());
        Assert.Equal(42, entry.GetProperty("attempt").GetProperty("details").GetProperty("profile_id").GetInt32());
        Assert.Equal(42, entry.GetProperty("attempt").GetProperty("diagnostics")[0]
            .GetProperty("details").GetProperty("profile_id").GetInt32());
        Assert.Equal("rejected", entry.GetProperty("journal").GetProperty("details").GetProperty("code").GetString());
        Assert.True(entry.GetProperty("journal").GetProperty("details").GetProperty("cursorAdvanced").GetBoolean());
        Assert.Equal(id, entry.GetProperty("deadLetter").GetProperty("id").GetGuid());
        var restored = JsonSerializer.Deserialize<InboxHistoryPage>(JsonSerializer.Serialize(page))!;
        Assert.Equal(page.Entries.Select(x => x.JournalId), restored.Entries.Select(x => x.JournalId));
        Assert.Equal(page.DeadLetter!.Value.GetProperty("id").GetGuid(),
            restored.DeadLetter!.Value.GetProperty("id").GetGuid());
    }

    [Fact]
    public async Task Invalid_dlq_history_has_raw_bytes_and_receipts_without_inbox_or_unrelated_null_links()
    {
        var host = HostFor();
        byte[] invalid = [0xff, 0x00];
        var first = await SaveRawAsync(host, invalid, "invalid-1");
        await SaveRawAsync(host, invalid, "invalid-1");
        await SaveRawAsync(host, [0xfe], "invalid-2");
        var page = (await HistoryAsync(host, first.DlqId!.Value, pageSize: 1))!;
        Assert.Null(page.Inbox);
        Assert.Null(page.ConsumerState);
        Assert.Equal(invalid, Convert.FromBase64String(page.DeadLetter!.Value.GetProperty("rawPayload").GetString()!));
        Assert.Single(page.Entries);
        Assert.Null(page.Entries[0].Attempt);
        Assert.NotNull(page.NextJournalId);
        var last = (await HistoryAsync(host, first.DlqId.Value, page.NextJournalId.Value, 1))!;
        Assert.Single(last.Entries);
        Assert.True(last.Entries[0].JournalId > page.Entries[0].JournalId);
        Assert.Null(last.NextJournalId);
        await using var scope = host.Services.CreateAsyncScope();
        var api = scope.ServiceProvider.GetRequiredService<InboxAdministration<TestDb>>();
        Assert.Null(await api.ReadDeadLetterAsync(Guid.NewGuid(), cancellationToken: Token));
        Assert.Null(await api.ReadHistoryAsync(Guid.NewGuid(), cancellationToken: Token));
    }

    [Fact]
    public async Task History_pagination_returns_each_journal_once_and_excludes_other_events()
    {
        var host = HostFor();
        await SeedAsync(host, 1);
        await SeedAsync(host, 1);
        await SeedAsync(host, 1);
        await SeedAsync(host, 1, "other");
        Assert.True(await ProcessAsync(host));
        Assert.True(await ProcessAsync(host));
        var id = await ScalarAsync<Guid>($"SELECT id FROM \"{_schema}\".inbox WHERE object_key = 'object-1'");
        await using var scope = host.Services.CreateAsyncScope();
        var api = scope.ServiceProvider.GetRequiredService<InboxAdministration<TestDb>>();
        var journals = new List<long>();
        long? cursor = 0;
        do
        {
            var page = (await api.ReadHistoryAsync(id, cursor.Value, 2, Token))!;
            Assert.Equal(id, page.Inbox!.Value.GetProperty("id").GetGuid());
            Assert.Equal("object-1", page.ConsumerState!.Value.GetProperty("objectKey").GetString());
            Assert.All(page.Entries, entry => Assert.Equal(id, entry.Journal.GetProperty("inboxId").GetGuid()));
            journals.AddRange(page.Entries.Select(x => x.JournalId));
            cursor = page.NextJournalId;
        } while (cursor is not null);
        Assert.Equal(5, journals.Count);
        Assert.Equal(journals, journals.Distinct().Order());
        Assert.NotNull(await api.ReadHistoryAsync(id, long.MaxValue, cancellationToken: Token));
        Assert.Empty((await api.ReadHistoryAsync(id, long.MaxValue, cancellationToken: Token))!.Entries);
    }

    [Theory]
    [InlineData(-1, 1)]
    [InlineData(0, 0)]
    [InlineData(0, 201)]
    public async Task History_rejects_unbounded_or_invalid_pagination(long after, int size)
    {
        var host = HostFor();
        await using var scope = host.Services.CreateAsyncScope();
        var api = scope.ServiceProvider.GetRequiredService<InboxAdministration<TestDb>>();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            api.ReadHistoryAsync(Guid.NewGuid(), after, size, Token)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            api.ReadDeadLetterAsync(Guid.NewGuid(), after, size, Token)
        );
    }

    [Fact]
    public async Task Administration_requires_a_separate_transaction_scope()
    {
        var host = HostFor();
        await using var scope = host.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDb>();
        await using var transaction = await db.Database.BeginTransactionAsync(Token);
        var api = scope.ServiceProvider.GetRequiredService<InboxAdministration<TestDb>>();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            api.ReadHistoryAsync(Guid.NewGuid(), cancellationToken: Token)
        );
    }

    private async Task<InboxHistoryPage?> HistoryAsync(IHost host, Guid id, long after = 0, int pageSize = 50)
    {
        await using var scope = host.Services.CreateAsyncScope();
        return await scope
            .ServiceProvider.GetRequiredService<InboxAdministration<TestDb>>()
            .ReadDeadLetterAsync(id, after, pageSize, Token);
    }

    private async Task<IntakeDecision> SaveRawAsync(IHost host, byte[] raw, string? identity = null)
    {
        var handler = host
            .Services.GetRequiredService<InboxCatalog<TestDb>>()
            .Handlers.Single(h => h.Id == "locations");
        await using var scope = host.Services.CreateAsyncScope();
        return await scope
            .ServiceProvider.GetRequiredService<InboxIntakeStore<TestDb>>()
            .SaveAsync(
                handler,
                handler.Subscriptions[0],
                new(raw, "{}", identity ?? Guid.NewGuid().ToString("N")),
                "intake-worker",
                Token
            );
    }
}

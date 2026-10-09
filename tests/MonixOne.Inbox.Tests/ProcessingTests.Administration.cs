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
        Assert.Equal(invalid, Convert.FromBase64String(page.DeadLetter!.Value.GetProperty("raw_payload").GetString()!));
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
            Assert.Equal("object-1", page.ConsumerState!.Value.GetProperty("object_key").GetString());
            Assert.All(page.Entries, entry => Assert.Equal(id, entry.Journal.GetProperty("inbox_id").GetGuid()));
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

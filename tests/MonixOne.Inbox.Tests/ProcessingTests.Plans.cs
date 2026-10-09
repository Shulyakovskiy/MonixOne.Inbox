using System.Text;
using LinqToDB;
using LinqToDB.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;
using Npgsql;

namespace MonixOne.Inbox.Tests;

public sealed partial class ProcessingTests
{
    [Fact]
    public async Task Active_head_and_cleanup_plans_use_indexes_with_large_history_and_mostly_idle_states()
    {
        var host = HostFor();
        await SqlAsync($$"""
            INSERT INTO "{{_schema}}".consumer_state (handler_id, producer, sequence_scope, object_key, last_sequence)
            SELECT 'locations', 'service', 'objects', 'idle-' || n, 157 FROM generate_series(1, 50000) n;
            INSERT INTO "{{_schema}}".inbox
                (id, handler_id, subscription_id, event_id, producer, sequence_scope, object_key, sequence,
                 event_type, occurred_at, envelope, raw_payload, source, run_id, status, completed_at)
            SELECT gen_random_uuid(), 'locations', 'events', 'historical-' || n, 'service', 'objects',
                'idle-' || n, 157, 'object.created', now(), '{}', decode('', 'hex'), '{}', gen_random_uuid(),
                'processed', now() - interval '100 days' FROM generate_series(1, 20000) n;
            """);
        for (var i = 0; i < 20; i++)
            await SeedAsync(host, 157, $"active-{i}");
        await SqlAsync($"ANALYZE \"{_schema}\".consumer_state; ANALYZE \"{_schema}\".inbox");
        await using var scope = host.Services.CreateAsyncScope();
        var processor = scope.ServiceProvider.GetRequiredService<InboxProcessor<TestDb>>();
        var cleanup = new InboxHistoryCleanup<TestDb>(host.Services.GetRequiredService<IServiceScopeFactory>(),
            host.Services.GetRequiredService<InboxCatalog<TestDb>>(), host.Services.GetRequiredService<InboxStartupBarrier>(),
            NullLogger<InboxHistoryCleanup<TestDb>>.Instance);
        await using var data = scope.ServiceProvider.GetRequiredService<TestDb>().CreateLinqToDBConnection();
        var catalog = host.Services.GetRequiredService<InboxCatalog<TestDb>>();
        var headSql = processor.CandidatesQuery(data, catalog.Handlers.Single()).ToSqlQuery();
        var cleanupSql = cleanup.SelectionQuery(data, DateTimeOffset.UtcNow.AddDays(-90),
            DateTimeOffset.UtcNow.AddDays(-30), 500).Select(i => new { i.Id, i.RunId }).ToSqlQuery();
        var headPlan = await ExplainAsync(headSql);
        var cleanupPlan = await ExplainAsync(cleanupSql);
        Assert.Contains("inbox_active_head_idx", headPlan);
        Assert.Contains("consumer_state_pk", headPlan);
        Assert.DoesNotContain("Seq Scan on consumer_state", headPlan);
        Assert.Contains("inbox_completed_idx", cleanupPlan);
        await SqlAsync($$"""
            INSERT INTO "{{_schema}}".inbox_dlq
                (id,related_inbox_id,handler_id,subscription_id,deduplication_key,category,event_id,producer,
                 sequence_scope,object_key,sequence,event_type,raw_payload,source,code,reason,worker_instance_id,
                 handler_type,package_version,created_at)
            SELECT gen_random_uuid(), i.id, i.handler_id, i.subscription_id, 'conflict:' || i.id, 'conflict',
                i.event_id, i.producer, i.sequence_scope, i.object_key, i.sequence, i.event_type,
                decode('', 'hex'), '{}', 'conflict', 'Conflicting fact', 'worker', 'handler', '0.1',
                CASE WHEN i.object_key LIKE '%0' THEN now() - interval '100 days' ELSE now() END
            FROM "{{_schema}}".inbox i WHERE i.status = 'processed';
            INSERT INTO "{{_schema}}".inbox_journal
                (inbox_id,dlq_id,handler_id,subscription_id,worker_instance_id,decision)
            SELECT related_inbox_id,id,handler_id,subscription_id,'worker','conflict' FROM "{{_schema}}".inbox_dlq;
            ANALYZE "{{_schema}}".inbox_dlq; ANALYZE "{{_schema}}".inbox_journal;
            """);
        var linkedPlan = await ExplainAsync(cleanupSql);
        // With 90% of history protected by fresh DLQ, PostgreSQL may prefer hash anti joins and a sequential scan.
        Assert.Contains("rows=500", linkedPlan);
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "artifacts");
        Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, "inbox-query-plans.txt"),
            "50,000 states; 20,000 completed messages; 20 active keys\n\nACTIVE HEAD\n" + headPlan +
            "\n\nCLEANUP\n" + cleanupPlan + "\n\nCLEANUP WITH 20,000 DLQ/JOURNAL LINKS\n" + linkedPlan, Token);
    }

    private async Task<string> ExplainAsync(QuerySql query)
    {
        await using var connection = new NpgsqlConnection(infrastructure.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + query.Sql, connection);
        foreach (var parameter in query.Parameters)
            command.Parameters.AddWithValue(parameter.Name!, parameter.Value ?? DBNull.Value);
        await using var reader = await command.ExecuteReaderAsync(Token);
        var plan = new StringBuilder();
        while (await reader.ReadAsync(Token))
            plan.AppendLine(reader.GetString(0));
        return plan.ToString();
    }
}

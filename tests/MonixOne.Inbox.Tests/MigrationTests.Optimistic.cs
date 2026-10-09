using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Migrations;
using Microsoft.Extensions.DependencyInjection;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox.Tests;

public sealed partial class MigrationTests
{
    [Fact]
    public async Task Upgrade_preserves_cursor_retry_received_time_and_terminal_evidence_without_old_blocks()
    {
        await ExecuteAsync($$"""
            CREATE SCHEMA "{{_schema}}";
            CREATE TABLE "{{_schema}}".schema_migrations
                (version integer PRIMARY KEY, name text NOT NULL, checksum varchar(64) NOT NULL,
                 minimum_reader_version integer NOT NULL, applied_at timestamptz NOT NULL);
            """);
        foreach (var migration in InboxMigration.All.Take(2))
        {
            await ExecuteAsync(migration.Sql.Replace("{{schema}}", $"\"{_schema}\"", StringComparison.Ordinal));
            await ExecuteAsync($$"""
                INSERT INTO "{{_schema}}".schema_migrations VALUES
                    ({{migration.Version}}, '{{migration.Name}}', '{{migration.Checksum}}', 1, now());
                """);
        }
        await ExecuteAsync($$"""
            INSERT INTO "{{_schema}}".consumer_state
                (handler_id, producer, sequence_scope, object_key, last_sequence, is_blocked, blocked_reason, gap_since, expected_sequence)
            VALUES ('locations','producer','scope','new',0,false,NULL,now() - interval '1 minute',155),
                ('locations','producer','scope','initialized',157,true,'rejected',NULL,NULL);
            INSERT INTO "{{_schema}}".inbox
                (id,handler_id,subscription_id,event_id,producer,sequence_scope,object_key,sequence,
                 event_type,occurred_at,envelope,raw_payload,source,run_id,status,attempt_count,received_at,next_attempt_at,code)
            VALUES ('00000000-0000-0000-0000-000000000001','locations','events','first','producer','scope','new',155,
                'object.created',now(),'{}',decode('', 'hex'),'{}','00000000-0000-0000-0000-000000000002','waiting_sequence',0,
                now() - interval '1 minute',NULL,'waiting_sequence'),
                ('00000000-0000-0000-0000-000000000003','locations','events','retry','producer','scope','initialized',158,
                'object.created',now(),'{}',decode('', 'hex'),'{}','00000000-0000-0000-0000-000000000004','retry',2,
                now() - interval '1 minute',now() + interval '1 hour','temporary'),
                ('00000000-0000-0000-0000-000000000005','locations','events','dead','producer','scope','initialized',160,
                'object.created',now(),'{}',decode('', 'hex'),'{}','00000000-0000-0000-0000-000000000006','dead',5,
                now() - interval '1 minute',NULL,'rejected');
            INSERT INTO "{{_schema}}".inbox_dlq
                (id,inbox_id,handler_id,subscription_id,deduplication_key,category,event_id,producer,sequence_scope,
                 object_key,sequence,event_type,raw_payload,source,code,reason,worker_instance_id,handler_type,
                 package_version,dlq_policy,run_id)
            VALUES ('00000000-0000-0000-0000-000000000007','00000000-0000-0000-0000-000000000005',
                'locations','events','terminal:old','terminal','dead','producer','scope','initialized',160,'object.created',
                decode('', 'hex'),'{}','rejected','terminal error','old worker','handler','0.1','BlockStream',
                '00000000-0000-0000-0000-000000000006');
            """);
        var received = await ScalarAsync<DateTime>($"SELECT received_at FROM \"{_schema}\".inbox WHERE event_id = 'retry'");
        var retry = await ScalarAsync<DateTime>($"SELECT next_attempt_at FROM \"{_schema}\".inbox WHERE event_id = 'retry'");
        await using var db = new TestDb(new DbContextOptionsBuilder<TestDb>().UseNpgsql(postgres.ConnectionString).Options);
        await InboxSchemaMigrator.EnsureReadyAsync(db, _schema, true, NullLogger.Instance, Token);
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".consumer_state WHERE last_sequence IS NULL AND object_key = 'new'"));
        Assert.Equal(157, await ScalarAsync<long>($"SELECT last_sequence FROM \"{_schema}\".consumer_state WHERE object_key = 'initialized'"));
        Assert.Equal(2, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".consumer_state WHERE version = 0"));
        Assert.Equal(received, await ScalarAsync<DateTime>($"SELECT received_at FROM \"{_schema}\".inbox WHERE event_id = 'retry'"));
        Assert.Equal(retry, await ScalarAsync<DateTime>($"SELECT next_attempt_at FROM \"{_schema}\".inbox WHERE event_id = 'retry'"));
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox WHERE event_id = 'dead' AND status = 'pending' AND code = 'terminal_policy_transition' AND attempt_count = 5"));
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox_dlq WHERE dlq_policy = 'BlockStream'"));
        Assert.Equal(InboxMigration.Initial.Checksum, await ScalarAsync<string>($"SELECT checksum FROM \"{_schema}\".schema_migrations WHERE version = 1"));
        Assert.Equal(0, await ScalarAsync<long>($"SELECT count(*) FROM information_schema.columns WHERE table_schema = '{_schema}' AND column_name IN ('is_blocked','gap_since','expected_sequence')"));
        using var host = CreateHost(autoMigrate: false);
        var handler = host.Services.GetRequiredService<InboxCatalog<TestDb>>().Handlers.Single();
        await ExecuteAsync($"UPDATE \"{_schema}\".inbox SET status = 'processed', completed_at = now() WHERE event_id = 'first'; " +
            $"UPDATE \"{_schema}\".consumer_state SET last_sequence = 155, version = 1 WHERE object_key = 'new'");
        // The future retry is the active head; the already-dead later event must not bypass it.
        await using (var scope = host.Services.CreateAsyncScope())
            Assert.False(await scope.ServiceProvider.GetRequiredService<InboxProcessor<TestDb>>()
                .ProcessOneAsync(handler, "new worker", Token));
        await ExecuteAsync($"UPDATE \"{_schema}\".inbox SET status = 'processed', completed_at = now() WHERE event_id IN ('retry','first'); " +
            $"UPDATE \"{_schema}\".consumer_state SET last_sequence = CASE WHEN object_key = 'new' THEN 155 ELSE 158 END, version = 1");
        await using (var scope = host.Services.CreateAsyncScope())
            Assert.True(await scope.ServiceProvider.GetRequiredService<InboxProcessor<TestDb>>()
                .ProcessOneAsync(handler, "new worker", Token));
        Assert.Equal(160, await ScalarAsync<long>($"SELECT last_sequence FROM \"{_schema}\".consumer_state WHERE object_key = 'initialized'"));
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox_journal WHERE decision = 'terminal_policy_transition'"));
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox_journal WHERE decision = 'gap_skipped' AND details ->> 'first_missing' = '159'"));
        Assert.Equal(1, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox_dlq"));
    }

    [Fact]
    public async Task An_active_head_index_with_the_same_name_but_incomplete_predicate_is_rejected()
    {
        await InstallAsync();
        await ExecuteAsync($"DROP INDEX \"{_schema}\".inbox_active_head_idx; " +
            $"CREATE INDEX inbox_active_head_idx ON \"{_schema}\".inbox " +
            "(handler_id, producer, sequence_scope, object_key, sequence) WHERE status = 'pending'");
        await using var db = new TestDb(new DbContextOptionsBuilder<TestDb>().UseNpgsql(postgres.ConnectionString).Options);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            InboxSchemaMigrator.EnsureReadyAsync(db, _schema, false, NullLogger.Instance, Token));
        Assert.Contains("inbox_active_head_idx", error.Message);
    }

    [Fact]
    public async Task A_constraint_with_the_original_name_but_wrong_columns_is_rejected()
    {
        await InstallAsync();
        await ExecuteAsync($"ALTER TABLE \"{_schema}\".inbox DROP CONSTRAINT inbox_event_uk; ALTER TABLE \"{_schema}\".inbox ADD CONSTRAINT inbox_event_uk UNIQUE (event_id)");
        await using var db = new TestDb(new DbContextOptionsBuilder<TestDb>().UseNpgsql(postgres.ConnectionString).Options);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            InboxSchemaMigrator.EnsureReadyAsync(db, _schema, false, NullLogger.Instance, Token));
        Assert.Contains("inbox_event_uk", error.Message);
    }
}

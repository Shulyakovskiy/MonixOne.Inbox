using System.Diagnostics;
using System.Text.Json;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Migrations;
using MonixOne.Inbox.Registration;
using NATS.Client.Core;
using Npgsql;

namespace MonixOne.Inbox.Tests;

[Collection("PostgreSQL")]
public sealed partial class MigrationTests(PostgreSqlFixture postgres) : IAsyncLifetime
{
    private readonly string _schema = $"inbox_{Guid.NewGuid():N}";
    private CancellationToken Token => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await ExecuteAsync($"DROP SCHEMA IF EXISTS \"{_schema}\" CASCADE");

    [Fact]
    public async Task Three_concurrent_hosts_create_schema_once_before_workers_start()
    {
        var hosts = Enumerable.Range(0, 3).Select(_ => CreateHost()).ToArray();
        try
        {
            await Task.WhenAll(hosts.Select(h => h.StartAsync(Token)));
            await Task.WhenAll(
                hosts.Select(h => h.Services.GetRequiredService<ProbeWorker>().Ready.Task.WaitAsync(Token))
            );
            Assert.Equal(3L, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".schema_migrations"));
            Assert.Equal(
                6L,
                await ScalarAsync<long>("SELECT count(*) FROM information_schema.tables WHERE table_schema = @schema")
            );
            Assert.False(await ScalarAsync<bool>("SELECT to_regclass('public.\"__EFMigrationsHistory\"') IS NOT NULL"));
            Assert.Equal(
                InboxMigration.Initial.Checksum,
                await ScalarAsync<string>($"SELECT checksum FROM \"{_schema}\".schema_migrations WHERE version = 1")
            );
        }
        finally
        {
            foreach (var host in hosts)
            {
                await host.StopAsync(Token);
                host.Dispose();
            }
        }
    }

    [Fact]
    public async Task Auto_migrate_disabled_validates_with_a_role_without_ddl_permissions()
    {
        await InstallAsync();
        var applied = await ScalarAsync<DateTime>(
            $"SELECT applied_at FROM \"{_schema}\".schema_migrations WHERE version = 1"
        );
        await ExecuteAsync(
            $"GRANT USAGE ON SCHEMA \"{_schema}\" TO inbox_reader; GRANT SELECT ON ALL TABLES "
                + $"IN SCHEMA \"{_schema}\" TO inbox_reader"
        );
        var connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString)
        {
            Username = "inbox_reader",
            Password = "inbox_reader",
        }.ConnectionString;

        using var host = CreateHost(autoMigrate: false, connectionString);
        await host.StartAsync(Token);
        await host.Services.GetRequiredService<ProbeWorker>().Ready.Task.WaitAsync(Token);
        await host.StopAsync(Token);
        Assert.Equal(
            applied,
            await ScalarAsync<DateTime>($"SELECT applied_at FROM \"{_schema}\".schema_migrations WHERE version = 1")
        );
    }

    [Fact]
    public async Task Auto_migrate_disabled_does_not_create_a_missing_schema()
    {
        using var host = CreateHost(autoMigrate: false);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("not installed", error.Message);
        Assert.False(await SchemaExistsAsync());
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
        Assert.Same(
            error,
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                host.Services.GetRequiredService<InboxStartupBarrier>().WaitAsync(Token)
            )
        );
    }

    [Fact]
    public async Task Missing_migration_in_validation_mode_is_not_reapplied()
    {
        await InstallAsync();
        await ExecuteAsync($"DELETE FROM \"{_schema}\".schema_migrations");
        using var host = CreateHost(autoMigrate: false);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("requires migration", error.Message);
        Assert.Equal(0L, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".schema_migrations"));
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
    }

    [Fact]
    public async Task Modified_checksum_prevents_startup_without_rewriting_history()
    {
        await InstallAsync();
        await ExecuteAsync($"UPDATE \"{_schema}\".schema_migrations SET checksum = repeat('0', 64)");
        using var host = CreateHost();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("checksum", error.Message);
        Assert.Equal(
            new string('0', 64),
            await ScalarAsync<string>($"SELECT checksum FROM \"{_schema}\".schema_migrations")
        );
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
    }

    [Fact]
    public async Task A_gap_in_migration_history_is_rejected()
    {
        await InstallAsync();
        await ExecuteAsync($"DELETE FROM \"{_schema}\".schema_migrations WHERE version = 2");
        using var host = CreateHost();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("noncontiguous", error.Message);
    }

    [Theory]
    [InlineData("ALTER TABLE {schema}.inbox DROP COLUMN raw_payload", "inbox.raw_payload")]
    [InlineData("ALTER TABLE {schema}.inbox ALTER COLUMN attempt_count TYPE bigint", "inbox.attempt_count")]
    [InlineData("ALTER TABLE {schema}.inbox DROP CONSTRAINT inbox_event_uk", "inbox_event_uk")]
    [InlineData("DROP INDEX {schema}.inbox_active_head_idx", "inbox_active_head_idx")]
    public async Task A_changed_schema_contract_prevents_startup(string mutation, string missing)
    {
        await InstallAsync();
        await ExecuteAsync(mutation.Replace("{schema}", $"\"{_schema}\"", StringComparison.Ordinal));
        using var host = CreateHost(autoMigrate: false);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains(missing, error.Message);
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
    }

    [Fact]
    public async Task Migration_failure_rolls_back_all_new_ddl_and_history()
    {
        await ExecuteAsync($"CREATE SCHEMA \"{_schema}\"; CREATE TABLE \"{_schema}\".inbox (existing_data text)");
        using var host = CreateHost();
        var error = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(Token));
        Assert.Equal(
            PostgresErrorCodes.DuplicateTable,
            Assert.IsType<PostgresException>(error.GetBaseException()).SqlState
        );
        Assert.False(await ScalarAsync<bool>($"SELECT to_regclass('\"{_schema}\".schema_migrations') IS NOT NULL"));
        Assert.False(await ScalarAsync<bool>($"SELECT to_regclass('\"{_schema}\".consumer_state') IS NOT NULL"));
        Assert.True(await ScalarAsync<bool>($"SELECT to_regclass('\"{_schema}\".inbox') IS NOT NULL"));
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
    }

    [Fact]
    public async Task Canceling_startup_while_waiting_for_lock_does_not_change_schema()
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        await using var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(@key)", connection, transaction);
        command.Parameters.AddWithValue("key", InboxSchemaMigrator.LockKey(connection.Database, _schema));
        await command.ExecuteNonQueryAsync(Token);

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(Token);
        using var host = CreateHost();
        var starting = host.StartAsync(stopping.Token);
        var timeout = Stopwatch.StartNew();
        while (
            !await ScalarAsync<bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND NOT granted)"
            )
        )
        {
            Assert.True(timeout.Elapsed < TimeSpan.FromSeconds(10), "Startup did not wait for the advisory lock.");
            await Task.Delay(TimeSpan.FromMilliseconds(20), Token);
        }

        await stopping.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => starting);
        Assert.False(await SchemaExistsAsync());
        Assert.False(host.Services.GetRequiredService<ProbeWorker>().Started);
        await transaction.RollbackAsync(Token);
        await InstallAsync(); // The failed startup leaked no session lock or partially committed DDL.
    }

    [Fact]
    public async Task Migration_uses_the_existing_context_connection_and_releases_its_transaction()
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Token);
        await using var db = new TestDb(new DbContextOptionsBuilder<TestDb>().UseNpgsql(connection).Options);
        LinqToDBForEFTools.Initialize();
        await InboxSchemaMigrator.EnsureReadyAsync(db, _schema, true, NullLogger.Instance, Token);
        Assert.Same(connection, db.Database.GetDbConnection());
        Assert.Null(db.Database.CurrentTransaction);
        Assert.Equal(System.Data.ConnectionState.Open, connection.State);
    }

    [Theory]
    [InlineData(
        "INSERT INTO {schema}.consumer_state (handler_id, producer, sequence_scope, "
            + "object_key, last_sequence) VALUES ('h','p','s','o',-1)"
    )]
    [InlineData(
        "INSERT INTO {schema}.consumer_state (handler_id, producer, sequence_scope, "
            + "object_key) VALUES ('h','p','s',repeat('é',1024))"
    )]
    public async Task Invalid_sequence_state_is_rejected_by_postgresql(string sql)
    {
        await InstallAsync();
        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            ExecuteAsync(sql.Replace("{schema}", $"\"{_schema}\"", StringComparison.Ordinal))
        );
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
    }

    private IHost CreateHost(bool autoMigrate = true, string? connectionString = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(o => o.ServicesStartConcurrently = true);
        builder.Services.AddDbContext<TestDb>(o =>
            o.UseNpgsql(connectionString ?? postgres.ConnectionString, provider => provider.EnableRetryOnFailure())
        );
        builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(
            NatsOpts.Default with
            {
                Url = postgres.NatsUrl,
            }
        ));
        // Register the worker first deliberately: readiness must be an explicit dependency.
        builder.Services.AddSingleton<ProbeWorker>();
        builder.Services.AddSingleton<IHostedService>(p => p.GetRequiredService<ProbeWorker>());
        builder
            .Services.AddOrderedInbox<TestDb>(o =>
            {
                o.Schema = _schema;
                o.AutoMigrate = autoMigrate;
            })
            .AddHandler<TestHandler>("locations", h => h.Subscribe("events", "EVENTS", "events.>", "locations"));
        return builder.Build();
    }

    [Theory]
    [InlineData("event-1", 2L, "inbox_event_uk")]
    [InlineData("event-2", 1L, "inbox_sequence_uk")]
    public async Task Duplicate_event_or_sequence_is_rejected_per_handler(
        string eventId,
        long sequence,
        string constraint
    )
    {
        await InstallAsync();
        await InsertEventAsync("locations", "event-1", 1);
        var error = await Assert.ThrowsAsync<PostgresException>(() => InsertEventAsync("locations", eventId, sequence));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal(constraint, error.ConstraintName);
        // Independent logical handlers may consume the same fact and source sequence.
        await InsertEventAsync("products", "event-1", 1);
        Assert.Equal(2L, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox"));
    }

    [Theory]
    [InlineData(0L, "pending")]
    [InlineData(-1L, "pending")]
    [InlineData(1L, "unknown")]
    [InlineData(1L, "retry")]
    public async Task Invalid_sequence_status_or_retry_without_a_due_time_is_rejected(long sequence, string status)
    {
        await InstallAsync();
        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            InsertEventAsync("locations", "event-1", sequence, status)
        );
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
    }

    [Fact]
    public async Task Maximum_bigint_sequence_is_stored_without_overflow()
    {
        await InstallAsync();
        await InsertEventAsync("locations", "event-max", long.MaxValue);
        Assert.Equal(long.MaxValue, await ScalarAsync<long>($"SELECT sequence FROM \"{_schema}\".inbox"));
    }

    [Fact]
    public async Task Dlq_and_journal_links_prevent_deletion_of_their_evidence()
    {
        await InstallAsync();
        await InsertEventAsync("locations", "event-1", 1);
        await ExecuteAsync(
            $"""
            UPDATE "{_schema}".inbox SET status = 'dead';
            INSERT INTO "{_schema}".inbox_dlq (
                id, inbox_id, handler_id, subscription_id, deduplication_key, category,
                event_id, producer, sequence_scope, object_key, sequence, event_type,
                envelope, raw_payload, source, code, reason, dlq_policy, run_id,
                worker_instance_id, handler_type, package_version)
            SELECT id, id, handler_id, subscription_id, 'terminal:' || id, 'terminal',
                event_id, producer, sequence_scope, object_key, sequence, event_type,
                envelope, raw_payload, source, 'rejected', 'Test rejection', 'BlockStream', run_id,
                'test-worker', 'TestHandler', 'test'
            FROM "{_schema}".inbox;
            INSERT INTO "{_schema}".inbox_journal (dlq_id, handler_id, worker_instance_id, decision)
            SELECT id, handler_id, 'test-worker', 'dead' FROM "{_schema}".inbox_dlq
            """
        );

        var original = await Assert.ThrowsAsync<PostgresException>(() =>
            ExecuteAsync($"DELETE FROM \"{_schema}\".inbox")
        );
        Assert.Equal(PostgresErrorCodes.RestrictViolation, original.SqlState);
        var dlq = await Assert.ThrowsAsync<PostgresException>(() =>
            ExecuteAsync($"DELETE FROM \"{_schema}\".inbox_dlq")
        );
        Assert.Equal(PostgresErrorCodes.RestrictViolation, dlq.SqlState);
        Assert.Equal(1L, await ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".inbox_journal"));
    }

    [Fact]
    public async Task Storage_preserves_array_json_and_original_bytes_for_custom_envelopes()
    {
        await InstallAsync();
        const string envelope = "[1, {\"changed\": true}]";
        await InsertEventAsync("locations", "event-array", 1, envelope: envelope);
        using var stored = JsonDocument.Parse(
            await ScalarAsync<string>($"SELECT envelope::text FROM \"{_schema}\".inbox")
        );
        Assert.Equal(JsonValueKind.Array, stored.RootElement.ValueKind);
        Assert.True(stored.RootElement[1].GetProperty("changed").GetBoolean());
        Assert.Equal(
            System.Text.Encoding.UTF8.GetBytes(envelope),
            await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox")
        );
    }

    private async Task InsertEventAsync(
        string handlerId,
        string eventId,
        long sequence,
        string status = "pending",
        string envelope = "{}"
    )
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand(
            $$"""
            INSERT INTO "{{_schema}}".consumer_state (handler_id, producer, sequence_scope, object_key)
            VALUES (@handler, 'producer', 'objects', 'object-1') ON CONFLICT DO NOTHING;
            INSERT INTO "{{_schema}}".inbox (
                id, handler_id, subscription_id, event_id, producer, sequence_scope, object_key,
                sequence, event_type, occurred_at, envelope, raw_payload, source, status, run_id)
            VALUES (@id, @handler, 'events', @event, 'producer', 'objects', 'object-1',
                @sequence, 'object.updated', CURRENT_TIMESTAMP, @envelope::jsonb, @raw, '{}'::jsonb, @status, @run)
            """,
            connection
        );
        command.Parameters.AddWithValue("id", Guid.NewGuid());
        command.Parameters.AddWithValue("handler", handlerId);
        command.Parameters.AddWithValue("event", eventId);
        command.Parameters.AddWithValue("sequence", sequence);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("run", Guid.NewGuid());
        command.Parameters.AddWithValue("envelope", envelope);
        command.Parameters.AddWithValue("raw", System.Text.Encoding.UTF8.GetBytes(envelope));
        await command.ExecuteNonQueryAsync(Token);
    }

    private async Task InstallAsync()
    {
        using var host = CreateHost();
        await host.StartAsync(Token);
        await host.Services.GetRequiredService<ProbeWorker>().Ready.Task.WaitAsync(Token);
        await host.StopAsync(Token);
    }

    private Task<bool> SchemaExistsAsync() =>
        ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = @schema)");

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Token);
    }

    private async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Token);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schema", _schema);
        return (T)(await command.ExecuteScalarAsync(Token))!;
    }

    private sealed class TestDb(DbContextOptions<TestDb> options) : DbContext(options);

    private sealed class TestHandler : IInboxHandler<TestDb>
    {
        public TestHandler() =>
            throw new InvalidOperationException("Schema startup must not construct business handlers.");

        public Task<InboxResult> HandleAsync(TestDb db, JsonElement message, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Schema startup must not call business handlers.");
    }

    private sealed class ProbeWorker(
        InboxStartupBarrier barrier,
        IServiceScopeFactory scopes,
        InboxCatalog<TestDb> catalog
    ) : BackgroundService
    {
        public bool Started { get; private set; }
        public TaskCompletionSource Ready { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await barrier.WaitAsync(stoppingToken);
            // Query the committed schema from a separate operation context, as a real worker will.
            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TestDb>();
            await using var data = db.CreateLinqToDBConnection();
            var count = await LinqToDB.Data.DataContextExtensions.ExecuteAsync<long>(
                data,
                $"SELECT count(*) FROM \"{catalog.Settings.Schema}\".schema_migrations",
                stoppingToken
            );
            Assert.True(count > 0);
            Started = true;
            Ready.TrySetResult();
        }
    }
}

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Migrations;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Npgsql;

namespace MonixOne.Inbox.Tests;

[Collection("PostgreSQL")]
public sealed class IntakeTests(PostgreSqlFixture infrastructure) : IAsyncLifetime
{
    private readonly string _schema = $"intake_{Guid.NewGuid():N}";
    private readonly string _stream = $"EVENTS_{Guid.NewGuid():N}";
    private readonly string _subject = $"test.{Guid.NewGuid():N}";
    private NatsConnection _nats = null!;
    private NatsJSContext _js = null!;
    private readonly List<IHost> _hosts = [];
    private CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _nats = new(NatsOpts.Default with { Url = infrastructure.NatsUrl });
        _js = new(_nats);
        await _js.CreateStreamAsync(
            new StreamConfig(_stream, [$"{_subject}.>"]) { Retention = StreamConfigRetention.Limits },
            cancellationToken: Token
        );
        LinqToDBForEFTools.Initialize();
        await using var db = new TestDb(
            new DbContextOptionsBuilder<TestDb>().UseNpgsql(infrastructure.ConnectionString).Options
        );
        await InboxSchemaMigrator.EnsureReadyAsync(db, _schema, true, NullLogger.Instance, Token);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in _hosts)
        {
            await host.StopAsync(Token);
            host.Dispose();
        }
        await _js.DeleteStreamAsync(_stream, Token);
        await _nats.DisposeAsync();
        await SqlAsync($"DROP SCHEMA \"{_schema}\" CASCADE");
    }

    [Fact]
    public async Task Documentation_envelope_is_received_from_jetstream_and_stored_without_DLQ()
    {
        var host = HostFor();
        await host.StartAsync(Token);
        var raw = """
            {
              "event_id": "profile:updated:42:157",
              "producer": "profiles",
              "sequence_scope": "profiles",
              "object_key": "42",
              "sequence": 157,
              "event_type": "profile.updated",
              "occurred_at": "2026-10-09T07:00:00Z",
              "payload": { "name": "Example" }
            }
            """u8.ToArray();

        await PublishAsync(raw);
        await UntilAsync(async () => await CountAsync("inbox_journal", "decision = 'received'") == 1);

        Assert.Equal(1, await CountAsync("inbox",
            "event_id = 'profile:updated:42:157' AND producer = 'profiles' AND sequence_scope = 'profiles' "
            + "AND object_key = '42' AND sequence = 157 AND event_type = 'profile.updated' "
            + "AND occurred_at = '2026-10-09T07:00:00Z'::timestamptz AND envelope -> 'payload' ->> 'name' = 'Example'"));
        Assert.Equal(raw, await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Fact]
    public async Task Independent_handlers_persist_full_bytes_and_metadata_and_deduplicate_semantic_json()
    {
        var host = HostFor(secondHandler: true);
        await host.StartAsync(Token);
        var raw = Event("created-1", 1);
        var headers = new NatsHeaders
        {
            ["Authorization"] = "Bearer private",
            ["traceparent"] = "00-trace",
            ["x-correlation-id"] = "correlation-1",
        };
        await PublishAsync(raw, headers: headers);
        await UntilAsync(async () => await CountAsync("inbox_journal", "decision = 'received'") == 2);
        using var json = JsonDocument.Parse(raw);
        var reversed = JsonSerializer.SerializeToUtf8Bytes(
            json.RootElement.EnumerateObject().Reverse().ToDictionary(x => x.Name, x => x.Value.Clone()),
            new JsonSerializerOptions { WriteIndented = true }
        );
        await PublishAsync(reversed);
        await UntilAsync(async () => await CountAsync("inbox_journal", "decision = 'duplicate'") == 2);
        Assert.Equal(2, await CountAsync("inbox"));
        Assert.Equal(2, await CountAsync("consumer_state", "last_sequence IS NULL"));
        Assert.Equal(0, await CountAsync("inbox_attempts"));
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT raw_payload, source::text FROM \"{_schema}\".inbox LIMIT 1",
            connection
        );
        await using var reader = await command.ExecuteReaderAsync(Token);
        Assert.True(await reader.ReadAsync(Token));
        Assert.Equal(raw, reader.GetFieldValue<byte[]>(0));
        var source = reader.GetString(1);
        Assert.DoesNotContain("private", source);
        Assert.Contains("[REDACTED]", source);
        Assert.Contains("correlation-1", source);
        Assert.Contains("stream_sequence", source);
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Fact]
    public async Task Several_subscriptions_share_the_producer_cursor_and_accept_out_of_order_events()
    {
        var host = HostFor(twoSubscriptions: true);
        await host.StartAsync(Token);
        await PublishAsync(Event("updated-2", 2), "b");
        await PublishAsync(Event("created-1", 1), "a");
        await UntilAsync(async () => await CountAsync("inbox") == 2);
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL"));
        Assert.Equal(2, await CountAsync("inbox", "status = 'pending' AND attempt_count = 0"));
    }

    [Fact]
    public async Task Three_replicas_share_one_durable_without_duplicate_inbox_rows()
    {
        var hosts = Enumerable.Range(0, 3).Select(_ => HostFor()).ToArray();
        await Task.WhenAll(hosts.Select(x => x.StartAsync(Token)));
        for (var i = 1; i <= 20; i++)
            await PublishAsync(Event($"updated-{i}", i));
        await UntilAsync(async () => await CountAsync("inbox") == 20);
        Assert.Equal(1, await CountAsync("consumer_state"));
        Assert.Equal(0, await CountAsync("inbox_dlq"));
        Assert.Equal(20, await CountAsync("inbox_journal", "decision = 'received'"));
    }

    [Fact]
    public async Task Conflict_links_both_originals_and_keeps_all_keys_available()
    {
        var host = HostFor();
        await host.StartAsync(Token);
        await PublishAsync(Event("created-a", 1, "a"));
        await PublishAsync(Event("updated-b", 2, "b"));
        await PublishAsync(Event("created-c", 1, "c"));
        await UntilAsync(async () => await CountAsync("inbox") == 3);
        var conflicting = Event("created-a", 2, "b");
        await PublishAsync(conflicting);
        await UntilAsync(async () => await CountAsync("inbox_dlq") == 1);
        Assert.Equal(3, await CountAsync("inbox"));
        Assert.Equal(3, await CountAsync("consumer_state", "last_sequence IS NULL"));
        Assert.Equal(1, await CountAsync("consumer_state", "object_key = 'c'"));
        Assert.Equal(
            2,
            await CountAsync("inbox_journal", "decision = 'conflict' AND inbox_id IS NOT NULL AND dlq_id IS NOT NULL")
        );
        Assert.Equal(conflicting, await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox_dlq"));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Same_id_with_changed_payload_or_different_id_at_same_sequence_is_a_conflict(bool sameId)
    {
        var host = HostFor();
        await host.StartAsync(Token);
        var original = Event("created-1", 1);
        await PublishAsync(original);
        await UntilAsync(async () => await CountAsync("inbox") == 1);
        await PublishAsync(Event(sameId ? "created-1" : "other-1", 1, value: 42));
        await UntilAsync(async () => await CountAsync("inbox_dlq") == 1);
        Assert.Equal(original, await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox"));
        Assert.Equal(1, await CountAsync("consumer_state", "last_sequence IS NULL"));
    }

    [Theory]
    [InlineData("{")]
    [InlineData("{}")]
    [InlineData("null")]
    [InlineData("{\"sequence\": 9223372036854775808}")]
    [InlineData("{\"data\": \"\\u0000\"}")]
    [InlineData("{\"data\": 1e9999999}")]
    public async Task Invalid_message_keeps_original_bytes_in_dlq_and_is_acknowledged(string json)
    {
        var host = HostFor();
        await host.StartAsync(Token);
        var raw = Encoding.UTF8.GetBytes(json);
        await PublishAsync(raw);
        await UntilAsync(async () => await CountAsync("inbox_dlq") == 1);
        Assert.Equal(raw, await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox_dlq"));
        Assert.Equal(0, await CountAsync("consumer_state"));
        Assert.Equal(0, await CountAsync("inbox"));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Redelivery_after_commit_before_ack_reuses_inbox_or_dlq(bool valid)
    {
        await CreateConsumerAsync();
        await PublishAsync(valid ? Event("created-1", 1) : [0xff, 0xfe]);
        var consumer = await _js.GetConsumerAsync(_stream, "locations", Token);
        var message = await NextAsync(consumer);
        var host = HostFor(); // Resolve store without starting transport, simulating a stopped process after COMMIT.
        var catalog = host.Services.GetRequiredService<InboxCatalog<TestDb>>();
        var handler = Assert.Single(catalog.Handlers);
        var stream = await _js.GetStreamAsync(_stream, cancellationToken: Token);
        var delivery = InboxIntake<TestDb>.CreateDelivery(
            message,
            Assert.Single(handler.Subscriptions),
            stream.Info.Created
        );
        await using (var scope = host.Services.CreateAsyncScope())
        {
            await scope
                .ServiceProvider.GetRequiredService<InboxIntakeStore<TestDb>>()
                .SaveAsync(handler, Assert.Single(handler.Subscriptions), delivery, "interrupted-worker", Token);
        }
        await host.StartAsync(Token);
        await UntilAsync(async () => await CountAsync("inbox_journal") >= 2);
        Assert.Equal(valid ? 1 : 0, await CountAsync("inbox"));
        Assert.Equal(valid ? 0 : 1, await CountAsync("inbox_dlq"));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Fact]
    public async Task Failed_database_write_is_not_acknowledged_and_recovers_after_permission_is_restored()
    {
        await SqlAsync(
            $"GRANT USAGE ON SCHEMA \"{_schema}\" TO inbox_reader; GRANT SELECT, INSERT, "
                + $"UPDATE ON ALL TABLES IN SCHEMA \"{_schema}\" TO inbox_reader; GRANT USAGE ON "
                + $"ALL SEQUENCES IN SCHEMA \"{_schema}\" TO inbox_reader; REVOKE INSERT ON "
                + $"\"{_schema}\".inbox FROM inbox_reader"
        );
        await CreateConsumerAsync();
        var cs = new NpgsqlConnectionStringBuilder(infrastructure.ConnectionString)
        {
            Username = "inbox_reader",
            Password = "inbox_reader",
        }.ConnectionString;
        var host = HostFor(connectionString: cs);
        await host.StartAsync(Token);
        await PublishAsync(Event("created-1", 1));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumRedelivered > 0);
        Assert.Equal(0, await CountAsync("inbox"));
        Assert.Equal(0, await CountAsync("consumer_state")); // Failed transaction rolled back its state insert too.
        Assert.Equal(1, (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending);
        await SqlAsync($"GRANT INSERT ON \"{_schema}\".inbox TO inbox_reader");
        await UntilAsync(async () => await CountAsync("inbox") == 1);
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Fact]
    public async Task Existing_incompatible_consumer_is_rejected_without_modification()
    {
        await _js.CreateConsumerAsync(
            _stream,
            new ConsumerConfig("locations")
            {
                AckPolicy = ConsumerConfigAckPolicy.None,
                FilterSubject = $"{_subject}.>",
            },
            Token
        );
        var host = HostFor();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("incompatible", error.Message);
        Assert.Equal(
            ConsumerConfigAckPolicy.None,
            (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.Config.AckPolicy
        );
    }

    [Fact]
    public async Task Scoped_envelope_adapter_uses_the_intake_scope_and_preserves_the_custom_JSON()
    {
        var contexts = new System.Collections.Concurrent.ConcurrentQueue<TestDb>();
        var host = HostFor(configureServices: services =>
        {
            services.AddSingleton(contexts);
            services.AddScoped<IInboxEnvelopeAdapter, TestEnvelopeAdapter>();
        });
        await host.StartAsync(Token);
        using var standard = JsonDocument.Parse(Event("created-157", 157));
        var raw = JsonSerializer.SerializeToUtf8Bytes(new { metadata = standard.RootElement, body = new { value = 99 } });
        await PublishAsync(raw);
        await UntilAsync(async () => await CountAsync("inbox") == 1);
        Assert.Equal(raw, await ScalarAsync<byte[]>($"SELECT raw_payload FROM \"{_schema}\".inbox"));
        Assert.Equal(1, await CountAsync("inbox", "sequence = 157 AND envelope -> 'body' ->> 'value' = '99'"));
        Assert.Single(contexts);
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    private sealed class TestEnvelopeAdapter(TestDb db,
        System.Collections.Concurrent.ConcurrentQueue<TestDb> contexts) : IInboxEnvelopeAdapter
    {
        public InboxEventMetadata Extract(JsonElement message)
        {
            contexts.Enqueue(db);
            // The adapter itself is scoped, and its dependencies come from this exact intake operation.
            Assert.Null(db.Database.CurrentTransaction);
            var original = message.GetProperty("metadata");
            return new(original.GetProperty("event_id").GetString()!, original.GetProperty("producer").GetString()!,
                original.GetProperty("sequence_scope").GetString()!, original.GetProperty("object_key").GetString()!,
                original.GetProperty("sequence").GetInt64(), original.GetProperty("event_type").GetString()!,
                original.GetProperty("occurred_at").GetDateTimeOffset());
        }
    }

    private IHost HostFor(
        bool secondHandler = false,
        bool twoSubscriptions = false,
        string? connectionString = null,
        Action<string, InboxHandlerOptions>? configure = null,
        Action<IServiceCollection>? configureServices = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddDbContext<TestDb>(options =>
            options.UseNpgsql(connectionString ?? infrastructure.ConnectionString)
        );
        builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(
            NatsOpts.Default with
            {
                Url = infrastructure.NatsUrl,
            }
        ));
        var inbox = builder.Services.AddOrderedInbox<TestDb>(o =>
        {
            o.Schema = _schema;
            o.AutoMigrate = false;
        });
        foreach (var id in secondHandler ? new[] { "locations", "products" } : ["locations"])
            inbox.AddHandler<NeverCalledHandler>(
                id,
                h =>
                {
                    h.Subscribe("events", _stream, twoSubscriptions ? $"{_subject}.a" : $"{_subject}.>", id);
                    if (twoSubscriptions)
                        h.Subscribe("other", _stream, $"{_subject}.b", $"{id}-other");
                    configure?.Invoke(id, h);
                }
            );
        // These tests isolate persistence/ACK. Dispatcher behavior has its own integration coverage.
        builder.Services.Remove(builder.Services.Single(d => d.ImplementationType == typeof(InboxDispatcher<TestDb>)));
        configureServices?.Invoke(builder.Services);
        var host = builder.Build();
        _hosts.Add(host);
        return host;
    }

    [Fact]
    public async Task Concurrent_cross_key_event_id_conflicts_are_serialized_between_replicas()
    {
        var hosts = Enumerable.Range(0, 3).Select(_ => HostFor()).ToArray();
        await Task.WhenAll(hosts.Select(x => x.StartAsync(Token)));
        for (var i = 0; i < 20; i++)
        {
            await PublishAsync(Event($"created-{i}", 1, $"a-{i}"));
            await PublishAsync(Event($"created-{i}", 1, $"b-{i}"));
        }
        await UntilAsync(async () => await CountAsync("inbox_dlq") == 20);
        Assert.Equal(20, await CountAsync("inbox"));
        Assert.Equal(40, await CountAsync("consumer_state", "last_sequence IS NULL"));
        await UntilAsync(async () => (await _js.GetConsumerAsync(_stream, "locations", Token)).Info.NumAckPending == 0);
    }

    [Fact]
    public async Task Work_queue_retention_is_rejected_before_creating_a_consumer()
    {
        await _js.DeleteStreamAsync(_stream, Token);
        await _js.CreateStreamAsync(
            new StreamConfig(_stream, [$"{_subject}.>"]) { Retention = StreamConfigRetention.Workqueue },
            cancellationToken: Token
        );
        var host = HostFor();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Token));
        Assert.Contains("WorkQueue", error.Message);
        Assert.Equal(0, (await _js.GetStreamAsync(_stream, cancellationToken: Token)).Info.State.ConsumerCount);
    }

    private async Task CreateConsumerAsync() =>
        await _js.CreateConsumerAsync(
            _stream,
            new ConsumerConfig("locations")
            {
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                FilterSubject = $"{_subject}.>",
                AckWait = TimeSpan.FromMilliseconds(200),
                MaxDeliver = -1,
            },
            Token
        );

    private async Task<INatsJSMsg<byte[]>> NextAsync(INatsJSConsumer consumer) =>
        await consumer.NextAsync(
            NatsRawSerializer<byte[]>.Default,
            new NatsJSNextOpts { Expires = TimeSpan.FromSeconds(5) },
            Token
        ) ?? throw new InvalidOperationException("Expected a delivery.");

    private async Task PublishAsync(byte[] bytes, string suffix = "a", NatsHeaders? headers = null) =>
        (
            await _js.PublishAsync(
                $"{_subject}.{suffix}",
                bytes,
                headers: headers,
                serializer: NatsRawSerializer<byte[]>.Default,
                cancellationToken: Token
            )
        ).EnsureSuccess();

    private static byte[] Event(string id, long sequence, string key = "object-1", int value = 1) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                event_id = id,
                producer = "service",
                sequence_scope = "objects",
                object_key = key,
                sequence,
                event_type = "object.created",
                occurred_at = "2026-10-08T01:00:00.1234567Z",
                payload = new { value },
            }
        );

    private async Task UntilAsync(Func<Task<bool>> ready)
    {
        var timer = Stopwatch.StartNew();
        while (!await ready())
        {
            Assert.True(timer.Elapsed < TimeSpan.FromSeconds(20), "Timed out waiting for durable intake.");
            await Task.Delay(50, Token);
        }
    }

    private Task<long> CountAsync(string table, string condition = "true") =>
        ScalarAsync<long>($"SELECT count(*) FROM \"{_schema}\".{table} WHERE {condition}");

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(infrastructure.ConnectionString);
        await connection.OpenAsync(Token);
        return connection;
    }

    private async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(Token))!;
    }

    private async Task SqlAsync(string sql)
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Token);
    }

    private sealed class TestDb(DbContextOptions<TestDb> options) : DbContext(options);

    private sealed class NeverCalledHandler : IInboxHandler<TestDb>
    {
        public NeverCalledHandler() =>
            throw new InvalidOperationException("Intake must not construct business handlers.");

        public Task<InboxResult> HandleAsync(TestDb db, JsonElement message, CancellationToken token) =>
            throw new NotSupportedException();
    }
}

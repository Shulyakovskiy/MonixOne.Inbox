using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Npgsql;
using Testcontainers.PostgreSql;

namespace MonixOne.Inbox.Tests;

[CollectionDefinition("PostgreSQL")]
public sealed class PostgreSqlCollection : ICollectionFixture<PostgreSqlFixture>;

public sealed class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:18-alpine")
        .WithDatabase("inbox_tests")
        .WithUsername("inbox_tests")
        .WithPassword("inbox_tests")
        .Build();

    private readonly IContainer _nats = new ContainerBuilder("nats:2.12-alpine")
        .WithCommand("--jetstream")
        .WithPortBinding(4222, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(4222))
        .Build();

    public string NatsUrl => $"nats://{_nats.Hostname}:{_nats.GetMappedPublicPort(4222)}";

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await Task.WhenAll(
            _container.StartAsync(TestContext.Current.CancellationToken),
            _nats.StartAsync(TestContext.Current.CancellationToken)
        );
        await using var nats = new NatsConnection(NatsOpts.Default with { Url = NatsUrl });
        await new NatsJSContext(nats).CreateStreamAsync(
            new StreamConfig("EVENTS", ["events.>"]) { Retention = StreamConfigRetention.Limits },
            cancellationToken: TestContext.Current.CancellationToken
        );
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand(
            "CREATE ROLE inbox_reader LOGIN PASSWORD 'inbox_reader'",
            connection
        );
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _nats.DisposeAsync();
        await _container.DisposeAsync();
    }
}

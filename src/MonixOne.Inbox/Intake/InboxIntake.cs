using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Registration;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace MonixOne.Inbox.Intake;

internal sealed class InboxIntake<TDbContext>(
    InboxCatalog<TDbContext> catalog,
    IServiceProvider services,
    InboxStartupBarrier barrier,
    IServiceScopeFactory scopes,
    ILogger<InboxIntake<TDbContext>> logger
) : BackgroundService
    where TDbContext : DbContext
{
    private readonly string _worker = $"{Environment.MachineName}:{Guid.NewGuid():N}";
    private readonly List<SubscriptionWorker> _subscriptions = [];

    internal async Task PrepareAsync(CancellationToken token)
    {
        foreach (var handler in catalog.Handlers.Where(x => x.Enabled))
        foreach (var subscription in handler.Subscriptions)
        {
            var connection = subscription.ConnectionName is null
                ? services.GetRequiredService<INatsConnection>()
                : services.GetRequiredKeyedService<INatsConnection>(subscription.ConnectionName);
            var worker = new SubscriptionWorker(handler, subscription, new NatsJSContext(connection));
            await BindAsync(worker, token);
            _subscriptions.Add(worker);
            logger.LogInformation(
                "OrderedInbox consumer ready: {HandlerId}/{SubscriptionId}, stream {Stream}, "
                    + "durable {DurableName}, worker {WorkerInstanceId}.",
                handler.Id,
                subscription.Id,
                subscription.Stream,
                subscription.DurableName,
                _worker
            );
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await barrier.WaitAsync(stoppingToken);
        // A distinct task per subscription; registration of several identical hosted-service types would be
        // deduplicated by DI.
        await Task.WhenAll(_subscriptions.Select(x => RunAsync(x, stoppingToken)));
    }

    private async Task BindAsync(SubscriptionWorker worker, CancellationToken token)
    {
        var options = worker.Settings;
        var stream = await worker.Context.GetStreamAsync(options.Stream, cancellationToken: token);
        if (stream.Info.Config.Retention == StreamConfigRetention.Workqueue)
            throw new InvalidOperationException(
                $"Stream '{options.Stream}' uses WorkQueue retention, incompatible with "
                    + $"independent inbox consumers. Use Limits or Interest retention."
            );
        if (stream.Info.Config.NoAck)
            throw new InvalidOperationException(
                $"Stream '{options.Stream}' must support acknowledged JetStream publication."
            );
        INatsJSConsumer consumer;
        try
        {
            consumer = await worker.Context.GetConsumerAsync(options.Stream, options.DurableName, token);
        }
        catch (NatsJSApiException error) when (error.Error.Code == 404)
        {
            try
            {
                // Create only: never update an existing application's consumer configuration.
                consumer = await worker.Context.CreateConsumerAsync(
                    options.Stream,
                    new ConsumerConfig(options.DurableName)
                    {
                        AckPolicy = ConsumerConfigAckPolicy.Explicit,
                        DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                        FilterSubject = options.Subject,
                        AckWait = TimeSpan.FromSeconds(30),
                        MaxDeliver = -1,
                        MaxAckPending = Math.Max(1024, options.BatchSize),
                    },
                    token
                );
            }
            catch (NatsJSApiException)
            {
                // Another replica may have won creation; read and validate its consumer.
                consumer = await worker.Context.GetConsumerAsync(options.Stream, options.DurableName, token);
            }
        }
        ValidateConsumer(consumer.Info.Config, options);
        worker.Consumer = consumer;
        worker.StreamCreated = stream.Info.Created;
    }

    private static void ValidateConsumer(ConsumerConfig config, InboxSubscriptionSettings options)
    {
        if (
            config.DurableName != options.DurableName
            || !string.IsNullOrEmpty(config.DeliverSubject)
            || config.AckPolicy != ConsumerConfigAckPolicy.Explicit
            || config.DeliverPolicy != ConsumerConfigDeliverPolicy.All
            || config.FilterSubject != options.Subject
            || config.FilterSubjects is { Count: > 0 }
            || config.HeadersOnly
            || config.MaxDeliver > 0
            || config.AckWait <= TimeSpan.Zero
            || config.Backoff is { Count: > 0 }
            || config.InactiveThreshold > TimeSpan.Zero
            || config.PauseUntil is { } pause && pause > DateTimeOffset.UtcNow
            || config.MaxBatch > 0 && config.MaxBatch < options.BatchSize
            || config.MaxExpires > TimeSpan.Zero && config.MaxExpires < TimeSpan.FromSeconds(2)
        )
            throw new InvalidOperationException(
                $"Consumer '{options.Stream}/{options.DurableName}' is incompatible: require "
                    + $"durable pull, explicit ACK, DeliverAll, the exact subject filter, full payload, "
                    + $"unlimited deliveries and no backoff, pause or automatic expiry. Existing "
                    + $"resources were not changed."
            );
    }

    private async Task RunAsync(SubscriptionWorker worker, CancellationToken token)
    {
        var failures = 0;
        while (!token.IsCancellationRequested)
        {
            try
            {
                if (worker.Consumer is null)
                    await BindAsync(worker, token);
                var inFlight = new HashSet<Task>();
                try
                {
                    await foreach (var message in worker.Consumer!.FetchAsync<byte[]>(
                        new NatsJSFetchOpts { MaxMsgs = worker.Settings.BatchSize, Expires = TimeSpan.FromSeconds(2) },
                        NatsRawSerializer<byte[]>.Default, token))
                    {
                        // Bounded operations; each owns its context. Drain the current fetch before another batch.
                        inFlight.Add(SaveAndAckAsync(worker, message, token));
                        if (inFlight.Count >= worker.Handler.Processing.IntakeConcurrency)
                        {
                            var finished = await Task.WhenAny(inFlight);
                            inFlight.Remove(finished);
                            await finished;
                        }
                    }
                }
                finally
                {
                    // Even after one failure, independent saves already running can finish and ACK.
                    await Task.WhenAll(inFlight);
                }
                failures = 0;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                // Stop this fetch on failure. Its remaining messages stay unacknowledged; no next
                // batch is requested during backoff.
                // Avoid exception text: provider errors may contain payload, SQL parameters or connection credentials.
                failures = Math.Min(failures + 1, 6);
                logger.LogWarning(
                    "OrderedInbox intake failed ({ErrorType}, SQLSTATE {SqlState}) for "
                        + "{HandlerId}/{SubscriptionId}; delivery may remain unacknowledged. Retrying in "
                        + "{DelaySeconds}s, worker {WorkerInstanceId}.",
                    error.GetType().Name,
                    (error as Npgsql.PostgresException)?.SqlState,
                    worker.Handler.Id,
                    worker.Settings.Id,
                    1 << (failures - 1),
                    _worker
                );
                worker.Consumer = null;
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1 << (failures - 1)), token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task SaveAndAckAsync(SubscriptionWorker worker, INatsJSMsg<byte[]> message, CancellationToken token)
    {
        var delivery = CreateDelivery(message, worker.Settings, worker.StreamCreated);
        await using var scope = scopes.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<InboxIntakeStore<TDbContext>>();
        var decision = await store.SaveAsync(worker.Handler, worker.Settings, delivery, _worker, token);
        await message.AckAsync(new AckOpts { DoubleAck = true }, token);
        logger.Log(decision.Kind is "conflict" or "invalid_message" ? LogLevel.Warning : LogLevel.Debug,
            "OrderedInbox {Decision}: {HandlerId}/{SubscriptionId}, inbox {InboxId}, DLQ {DlqId}, worker {Worker}.",
            decision.Kind, worker.Handler.Id, worker.Settings.Id, decision.InboxId, decision.DlqId, _worker);
    }

    internal static InboxDelivery CreateDelivery(
        INatsJSMsg<byte[]> message,
        InboxSubscriptionSettings settings,
        DateTimeOffset streamCreated
    )
    {
        var metadata =
            message.Metadata ?? throw new InvalidOperationException("JetStream delivery metadata is required.");
        var headers = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        if (message.Headers is { } sourceHeaders)
            foreach (var header in sourceHeaders)
                headers[header.Key.Replace("\0", "\\u0000", StringComparison.Ordinal)] = Sensitive(header.Key)
                    ? ["[REDACTED]"]
                    : header.Value.Select(x => (x ?? "").Replace("\0", "\\u0000", StringComparison.Ordinal)).ToArray();
        var source = JsonSerializer.Serialize(
            new
            {
                connection = settings.ConnectionName ?? "default",
                stream = metadata.Stream,
                consumer = metadata.Consumer,
                domain = metadata.Domain,
                subject = message.Subject,
                stream_created_at = streamCreated,
                stream_sequence = metadata.Sequence.Stream,
                consumer_sequence = metadata.Sequence.Consumer,
                delivery_count = metadata.NumDelivered,
                pending = metadata.NumPending,
                published_at = metadata.Timestamp,
                received_at = DateTimeOffset.UtcNow,
                headers,
            }
        );
        // Consumer/delivery count are deliberately excluded: overlapping subscriptions/redeliveries share one DLQ fact.
        var identity = JsonSerializer.Serialize(
            new[]
            {
                settings.ConnectionName ?? "default",
                metadata.Domain,
                metadata.Stream,
                streamCreated.ToString("O"),
                metadata.Sequence.Stream.ToString(System.Globalization.CultureInfo.InvariantCulture),
            }
        );
        return new(message.Data ?? [], source, identity);
    }

    private static bool Sensitive(string key) =>
        new[] { "authorization", "cookie", "password", "secret", "token", "credential", "api-key", "apikey" }.Any(x =>
            key.Contains(x, StringComparison.OrdinalIgnoreCase)
        );

    private sealed class SubscriptionWorker(
        InboxHandlerRegistration<TDbContext> handler,
        InboxSubscriptionSettings settings,
        NatsJSContext context
    )
    {
        internal InboxHandlerRegistration<TDbContext> Handler { get; } = handler;
        internal InboxSubscriptionSettings Settings { get; } = settings;
        internal NatsJSContext Context { get; } = context;
        internal INatsJSConsumer? Consumer { get; set; }
        internal DateTimeOffset StreamCreated { get; set; }
    }
}

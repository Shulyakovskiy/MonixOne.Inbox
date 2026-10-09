using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Migrations;

namespace MonixOne.Inbox.Registration;

internal sealed class InboxStartup<TDbContext>(
    InboxCatalog<TDbContext> catalog,
    IServiceScopeFactory scopes,
    InboxStartupBarrier barrier,
    InboxIntake<TDbContext> intake,
    ILogger<InboxStartup<TDbContext>> logger
) : IHostedLifecycleService
    where TDbContext : DbContext
{
    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await InitializeAsync(cancellationToken);
            await intake.PrepareAsync(cancellationToken);
            barrier.Complete();
        }
        catch (Exception error)
        {
            barrier.Fail(error);
            if (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                logger.LogError(
                    error,
                    "OrderedInbox initialization failed for schema {Schema}; message intake cannot start.",
                    catalog.Settings.Schema
                );
            throw;
        }
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        foreach (var handler in catalog.Handlers)
        {
            if (!handler.Enabled)
            {
                logger.LogInformation("OrderedInbox handler {HandlerId} is disabled.", handler.Id);
                continue;
            }

            logger.LogDebug(
                "Configured OrderedInbox handler {HandlerId}: type {HandlerType}, parallel "
                    + "objects {MaxParallelObjects}, timeout {HandlerTimeout}, attempts {MaxAttempts}, "
                    + "gap timeout {GapTimeout}.",
                handler.Id,
                handler.HandlerType.FullName,
                handler.Processing.MaxParallelObjects,
                handler.Processing.HandlerTimeout,
                handler.Processing.Retry.MaxAttempts,
                handler.Processing.GapTimeout
            );
            foreach (var subscription in handler.Subscriptions)
                logger.LogDebug(
                    "Configured OrderedInbox subscription {HandlerId}/{SubscriptionId}: stream "
                        + "{Stream}, subject {Subject}, durable {DurableName}, connection {ConnectionName},"
                        + " batch {BatchSize}.",
                    handler.Id,
                    subscription.Id,
                    subscription.Stream,
                    subscription.Subject,
                    subscription.DurableName,
                    subscription.ConnectionName ?? "default",
                    subscription.BatchSize
                );
        }

        if (catalog.Handlers.All(h => !h.Enabled))
        {
            logger.LogInformation("OrderedInbox has no enabled handlers.");
            return;
        }

        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        if (db.Database.ProviderName != "Npgsql.EntityFrameworkCore.PostgreSQL")
        {
            logger.LogError(
                "OrderedInbox requires UseNpgsql for application context {DbContextType}.",
                typeof(TDbContext).FullName
            );
            throw new InvalidOperationException(
                "OrderedInbox requires an application DbContext configured with UseNpgsql."
            );
        }

        LinqToDBForEFTools.Initialize();
        logger.LogInformation(
            "OrderedInbox registration validated: {HandlerCount} enabled handlers, "
                + "{SubscriptionCount} subscriptions, schema {Schema}, AutoMigrate {AutoMigrate}.",
            catalog.Handlers.Count(h => h.Enabled),
            catalog.Handlers.Where(h => h.Enabled).Sum(h => h.Subscriptions.Length),
            catalog.Settings.Schema,
            catalog.Settings.AutoMigrate
        );
        await InboxSchemaMigrator.EnsureReadyAsync(
            db,
            catalog.Settings.Schema,
            catalog.Settings.AutoMigrate,
            logger,
            cancellationToken
        );
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

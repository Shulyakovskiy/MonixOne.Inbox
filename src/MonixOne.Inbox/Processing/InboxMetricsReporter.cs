using LinqToDB;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox.Processing;

internal sealed class InboxMetricsReporter<TDbContext>(
    IServiceScopeFactory scopes,
    InboxCatalog<TDbContext> catalog,
    InboxStartupBarrier barrier,
    InboxMetrics metrics,
    ILogger<InboxMetricsReporter<TDbContext>> logger
) : BackgroundService
    where TDbContext : DbContext
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await barrier.WaitAsync(stoppingToken);
        if (catalog.Handlers.All(x => !x.Enabled))
            return;
        // A fixed low-frequency refresh avoids SQL in exporter callbacks and extra configuration knobs.
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                await RefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error)
            {
                // Keep the previous snapshot; its age exposes stale readings during a database outage.
                logger.LogWarning(
                    "OrderedInbox metric snapshot refresh failed ({ErrorType}) for schema {Schema}.",
                    error.GetType().Name,
                    catalog.Settings.Schema
                );
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    internal async Task RefreshAsync(CancellationToken token)
    {
        var handlers = catalog.Handlers.Where(x => x.Enabled).Select(x => x.Id).ToArray();
        if (handlers.Length == 0)
            return;
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var states = await db
            .Database.CreateExecutionStrategy()
            .ExecuteAsync(
                async cancellation =>
                {
                    await using var data = db.CreateLinqToDBConnection();
                    if (db.Database.GetCommandTimeout() is { } timeout)
                        data.CommandTimeout = timeout;
                    return await data.GetTable<InboxRow>()
                        .SchemaName(catalog.Settings.Schema)
                        .Where(x => Enumerable.Contains(handlers, x.HandlerId))
                        .Where(x => x.Status == "pending" || x.Status == "retry")
                        .GroupBy(x => x.HandlerId)
                        .Select(g => new InboxStateMeasurement(g.Key, g.LongCount()))
                        .ToListAsyncLinqToDB(cancellation);
                },
                token
            );
        metrics.UpdateSnapshot(
            new(
                catalog.Settings.Schema,
                DateTimeOffset.UtcNow,
                handlers.Select(id => states.SingleOrDefault(x => x.HandlerId == id) ?? new(id, 0)).ToArray()
            )
        );
    }
}

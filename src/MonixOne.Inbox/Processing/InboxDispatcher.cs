using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox.Processing;

internal sealed class InboxDispatcher<TDbContext>(
    IServiceScopeFactory scopes,
    InboxCatalog<TDbContext> catalog,
    InboxStartupBarrier barrier,
    ILogger<InboxDispatcher<TDbContext>> logger
) : BackgroundService
    where TDbContext : DbContext
{
    private readonly string _worker = $"{Environment.MachineName}:{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await barrier.WaitAsync(stoppingToken);
        // Each slot owns a fresh scope per operation; the database coordinates replicas and object ordering.
        var tasks = catalog
            .Handlers.Where(x => x.Enabled)
            .SelectMany(handler =>
                Enumerable.Range(0, handler.Processing.MaxParallelObjects).Select(_ => RunAsync(handler, stoppingToken))
            );
        await Task.WhenAll(tasks);
    }

    private async Task RunAsync(InboxHandlerRegistration<TDbContext> handler, CancellationToken token)
    {
        var failures = 0;
        while (!token.IsCancellationRequested)
        {
            var delay = handler.Processing.PollInterval;
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var processor = scope.ServiceProvider.GetRequiredService<InboxProcessor<TDbContext>>();
                var processed = await processor.ProcessOneAsync(handler, _worker, token);
                failures = 0;
                if (processed)
                    continue;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                failures = Math.Min(failures + 1, 6);
                delay = TimeSpan.FromSeconds(1 << (failures - 1));
                logger.LogWarning(
                    "OrderedInbox processing infrastructure failed ({ErrorType}) for {HandlerId}; "
                        + "retrying in {Delay}, worker {WorkerInstanceId}.",
                    error.GetType().Name,
                    handler.Id,
                    delay,
                    _worker
                );
            }
            try
            {
                await Task.Delay(delay, token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
        }
    }
}

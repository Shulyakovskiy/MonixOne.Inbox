using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.PostgreSQL;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox.Registration;
using MonixOne.Inbox.Intake;

namespace MonixOne.Inbox.Processing;

internal sealed class InboxHistoryCleanup<TDbContext>(IServiceScopeFactory scopes, InboxCatalog<TDbContext> catalog,
    InboxStartupBarrier barrier, ILogger<InboxHistoryCleanup<TDbContext>> logger) : BackgroundService
    where TDbContext : DbContext
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await barrier.WaitAsync(stoppingToken);
        if (!catalog.Settings.HistoryCleanup.Enabled || catalog.Handlers.All(x => !x.Enabled))
            return;
        using var timer = new PeriodicTimer(catalog.Settings.HistoryCleanup.Interval);
        do
        {
            try
            {
                for (var batch = 0; batch < 10 && !stoppingToken.IsCancellationRequested; batch++)
                {
                    if (await CleanupBatchAsync(stoppingToken) == 0)
                        break;
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                logger.LogWarning("OrderedInbox history cleanup failed ({ErrorType}) in {Schema}; next cycle will retry.",
                    error.GetType().Name, catalog.Settings.Schema);
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    internal IQueryable<InboxRow> SelectionQuery(DataConnection data, DateTimeOffset dead,
        DateTimeOffset completed, int batch)
    {
        var schema = catalog.Settings.Schema;
        var dlq = data.GetTable<InboxDlqRow>().SchemaName(schema);
        var journal = data.GetTable<InboxJournalRow>().SchemaName(schema);
        var states = data.GetTable<InboxStateRow>().SchemaName(schema);
        var latest = dead > completed ? dead : completed;
        return data.GetTable<InboxRow>().SchemaName(schema)
            .Where(i => (i.Status == "processed" || i.Status == "skipped" || i.Status == "dead") &&
                i.CompletedAt < latest && i.CompletedAt < (i.Status == "dead" ? dead : completed) &&
                !dlq.Any(d => d.InboxId == i.Id && d.CreatedAt >= dead) &&
                !dlq.Any(d => d.RelatedInboxId == i.Id && d.CreatedAt >= dead) &&
                !(from j in journal join d in dlq on j.DlqId equals d.Id
                    where j.InboxId == i.Id && d.CreatedAt >= dead select j.Id).Any() &&
                !states.Any(s => s.HandlerId == i.HandlerId && s.Producer == i.Producer &&
                    s.SequenceScope == i.SequenceScope && s.ObjectKey == i.ObjectKey &&
                    (s.LastSequence == null || s.LastSequence < i.Sequence)))
            .OrderBy(i => i.CompletedAt).ThenBy(i => i.Id).Take(batch)
            .AsPostgreSQL().SubQueryTableHint(PostgreSQLHints.ForUpdate, PostgreSQLHints.SkipLocked);
    }

    internal async Task<int> CleanupBatchAsync(CancellationToken token)
    {
        var options = catalog.Settings.HistoryCleanup;
        if (!options.Enabled)
            return 0;
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        return await db.Database.CreateExecutionStrategy().ExecuteAsync(async cancellation =>
        {
            await using var transaction = await db.Database.BeginTransactionAsync(cancellation);
            await using var data = db.CreateLinqToDBConnection(transaction);
            await InboxPostgreSql.SetLockTimeoutAsync(data, cancellation);
            var schema = catalog.Settings.Schema;
            var now = await data.SelectAsync(() => InboxPostgreSql.TransactionTimestamp(), cancellation);
            var dead = now - options.DeadLetterRetention;
            var completed = now - options.CompletedRetention;
            var inbox = data.GetTable<InboxRow>().SchemaName(schema);
            var dlq = data.GetTable<InboxDlqRow>().SchemaName(schema);
            var journal = data.GetTable<InboxJournalRow>().SchemaName(schema);
            var attempts = data.GetTable<InboxAttemptRow>().SchemaName(schema);
            var events = await SelectionQuery(data, dead, completed, options.BatchSize)
                .Select(i => new { i.Id, i.RunId }).ToListAsyncLinqToDB(cancellation);
            // Lock the eligible terminal rows for this short cleanup transaction. Processing never selects them.
            var letters = await dlq.Where(d => d.CreatedAt < dead &&
                    !inbox.Any(i => i.Id == d.InboxId && (i.Status == "pending" || i.Status == "retry")) &&
                    !inbox.Any(i => i.Id == d.RelatedInboxId && (i.Status == "pending" || i.Status == "retry")) &&
                    !(from j in journal join i in inbox on j.InboxId equals i.Id
                        where j.DlqId == d.Id && (i.Status == "pending" || i.Status == "retry") select j.Id).Any())
                .OrderBy(d => d.CreatedAt).ThenBy(d => d.Id).Take(options.BatchSize)
                .AsPostgreSQL().SubQueryTableHint(PostgreSQLHints.ForUpdate, PostgreSQLHints.SkipLocked)
                .Select(d => d.Id).ToArrayAsyncLinqToDB(cancellation);
            // A conflict may refer to multiple originals through the journal. Delete its links only after retention.
            if (letters.Length > 0)
            {
                await journal.Where(j => j.DlqId != null && ((IEnumerable<Guid>)letters).Contains(j.DlqId.Value)).DeleteAsync(cancellation);
                await dlq.Where(d => letters.AsEnumerable().Contains(d.Id) && d.CreatedAt < dead).DeleteAsync(cancellation);
            }

            var deleted = 0;
            if (events.Count > 0)
            {
                var selectedIds = events.Select(i => i.Id).ToArray();
                // Recheck references after removing expired DLQ. The selected inbox rows remain FOR UPDATE locked,
                // including against concurrent FK inserts, until all dependent history and inbox rows are deleted.
                var eligible = await inbox.Where(i => selectedIds.AsEnumerable().Contains(i.Id) &&
                        (i.Status == "processed" || i.Status == "skipped" || i.Status == "dead") &&
                        i.CompletedAt < (i.Status == "dead" ? dead : completed) &&
                        !dlq.Any(d => d.InboxId == i.Id || d.RelatedInboxId == i.Id) &&
                        !journal.Any(j => j.InboxId == i.Id && j.DlqId != null))
                    .Select(i => new { i.Id, i.RunId }).ToListAsyncLinqToDB(cancellation);
                var runs = events.ToDictionary(i => i.Id, i => i.RunId);
                var ids = eligible.Where(i => runs[i.Id] == i.RunId).Select(i => i.Id).ToArray();
                if (ids.Length > 0)
                {
                    await journal.Where(j => (j.InboxId != null && ids.AsEnumerable().Contains(j.InboxId.Value)) ||
                        attempts.Any(a => a.AttemptId == j.AttemptId && ids.AsEnumerable().Contains(a.InboxId)))
                        .DeleteAsync(cancellation);
                    await attempts.Where(a => ids.AsEnumerable().Contains(a.InboxId)).DeleteAsync(cancellation);
                    deleted = await inbox.Where(i => ids.AsEnumerable().Contains(i.Id)).DeleteAsync(cancellation);
                }
            }
            await transaction.CommitAsync(cancellation);
            return deleted + letters.Length;
        }, token);
    }
}

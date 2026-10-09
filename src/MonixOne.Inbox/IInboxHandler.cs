using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace MonixOne.Inbox;

public interface IInboxHandler<in TDbContext>
    where TDbContext : DbContext
{
    /// <summary>
    /// Applies one event using the application context from the current attempt's scope.
    /// </summary>
    /// <remarks>
    /// The package owns the transaction and context lifetime. Only Applied commits business changes;
    /// other results roll them back. External side effects must use the application's transactional outbox.
    /// The same event can invoke this callback concurrently on different replicas; only one transaction commits.
    /// Keep all writes in the supplied context/transaction, honor cancellation and await all work before returning.
    /// </remarks>
    Task<InboxResult> HandleAsync(TDbContext db, JsonElement message, CancellationToken cancellationToken);
}

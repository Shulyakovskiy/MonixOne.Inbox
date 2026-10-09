using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox;

public sealed class OrderedInboxBuilder<TDbContext>
    where TDbContext : DbContext
{
    private readonly IServiceCollection _services;
    private readonly InboxRegistration<TDbContext> _registration;

    internal OrderedInboxBuilder(IServiceCollection services, InboxRegistration<TDbContext> registration)
    {
        _services = services;
        _registration = registration;
    }

    public OrderedInboxBuilder<TDbContext> AddHandler<THandler>(
        string handlerId,
        Action<InboxHandlerOptions>? configure = null
    )
        where THandler : class, IInboxHandler<TDbContext>
    {
        CheckHandlerId(handlerId);
        _services.TryAddScoped<THandler>();
        _registration.Handlers.Add(
            new(
                handlerId,
                typeof(THandler),
                // The caller supplies an operation scope, never the worker's singleton provider.
                static services =>
                    services.GetRequiredService<THandler>(),
                configure
            )
        );

        return this;
    }

    public OrderedInboxBuilder<TDbContext> AddSubscription(
        string handlerId,
        string subscriptionId,
        Action<InboxSubscriptionOptions> configure
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(handlerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionId);
        ArgumentNullException.ThrowIfNull(configure);
        if (
            _registration.Subscriptions.Any(s =>
                string.Equals(s.HandlerId, handlerId, StringComparison.OrdinalIgnoreCase)
                && string.Equals(s.Id, subscriptionId, StringComparison.OrdinalIgnoreCase)
            )
        )
            throw new ArgumentException(
                $"Subscription '{handlerId}/{subscriptionId}' is already registered.",
                nameof(subscriptionId)
            );

        _registration.Subscriptions.Add(new(handlerId, subscriptionId, configure));
        return this;
    }

    private void CheckHandlerId(string handlerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(handlerId);
        if (_registration.Handlers.Any(h => string.Equals(h.Id, handlerId, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"Handler '{handlerId}' is already registered.", nameof(handlerId));
    }

}

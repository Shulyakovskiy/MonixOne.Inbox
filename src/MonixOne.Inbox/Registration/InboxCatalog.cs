using System.Collections.Immutable;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MonixOne.Inbox.Registration;

internal sealed class InboxCatalog<TDbContext>
    where TDbContext : DbContext
{
    internal InboxCatalog(
        InboxRegistration<TDbContext> registration,
        IServiceCollection services,
        IServiceProvider provider
    )
    {
        var configuration = new OrderedInboxOptions();
        try
        {
            foreach (var section in registration.Configurations)
                section.Bind(configuration, static o => o.ErrorOnUnknownConfiguration = true);
        }
        catch (InvalidOperationException error)
        {
            throw new OptionsValidationException(
                "OrderedInbox",
                typeof(OrderedInboxOptions),
                [error.GetBaseException().Message]
            );
        }

        // Bind once, then mutate that same configuration with explicit code overrides.
        foreach (var configure in registration.Configure)
            configure(configuration);

        Settings = new(configuration.Schema ?? "ordered_inbox", configuration.AutoMigrate ?? true,
            InboxCleanupSettings.Defaults.Apply(configuration.HistoryCleanup));
        var handlers = ImmutableArray.CreateBuilder<InboxHandlerRegistration<TDbContext>>();
        var errors = new List<string>();
        CheckKnownHandlers(configuration, registration, errors);

        foreach (var descriptor in registration.Handlers)
        {
            var local = configuration.Handlers.GetValueOrDefault(descriptor.Id) ?? new InboxHandlerOptions();
            descriptor.Configure?.Invoke(local);
            var processing = InboxProcessingSettings.Defaults.Apply(configuration.Defaults).Apply(local);
            var subscriptions = new Dictionary<string, InboxSubscriptionSettings>(StringComparer.Ordinal);
            ApplySubscriptions(subscriptions, local);
            foreach (var subscription in registration.Subscriptions.Where(s => s.HandlerId == descriptor.Id))
            {
                var options = new InboxSubscriptionOptions();
                subscription.Configure(options);
                ApplySubscription(subscriptions, subscription.Id, options);
            }

            handlers.Add(
                new(
                    descriptor.Id,
                    descriptor.HandlerType,
                    descriptor.Resolve,
                    local.Enabled ?? true,
                    processing,
                    subscriptions.Values.ToImmutableArray()
                )
            );
        }

        foreach (var subscription in registration.Subscriptions)
            if (registration.Handlers.All(h => h.Id != subscription.HandlerId))
                errors.Add($"Subscription '{subscription.HandlerId}/{subscription.Id}' references an unknown handler.");

        Handlers = handlers.ToImmutable();
        InboxSettingsValidator.Validate(Settings, Handlers, services, provider, errors);
        if (errors.Count > 0)
            throw new OptionsValidationException("OrderedInbox", typeof(OrderedInboxOptions), errors);
    }

    internal InboxSettings Settings { get; }
    internal ImmutableArray<InboxHandlerRegistration<TDbContext>> Handlers { get; }

    private static void CheckKnownHandlers(
        OrderedInboxOptions options,
        InboxRegistration<TDbContext> registration,
        List<string> errors
    )
    {
        if (options.Handlers is null)
            throw new ArgumentException("OrderedInbox.Handlers cannot be null.");

        foreach (var handlerId in options.Handlers.Keys)
            if (registration.Handlers.All(h => h.Id != handlerId))
                errors.Add($"Settings reference unknown handler '{handlerId}'. Register its C# handler explicitly.");
    }

    private static void ApplySubscriptions(
        Dictionary<string, InboxSubscriptionSettings> target,
        InboxHandlerOptions? options
    )
    {
        if (options is null)
            return;
        if (options.Subscriptions is null)
            throw new ArgumentException("Handler.Subscriptions cannot be null.");

        foreach (var (id, subscription) in options.Subscriptions)
            ApplySubscription(target, id, subscription);
    }

    private static void ApplySubscription(
        Dictionary<string, InboxSubscriptionSettings> target,
        string id,
        InboxSubscriptionOptions options
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        var current = target.GetValueOrDefault(id) ?? new(id, null, "", "", "", 32);
        target[id] = current.Apply(options);
    }
}

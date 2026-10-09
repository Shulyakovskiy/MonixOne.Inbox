using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox;

public static class OrderedInboxServiceCollectionExtensions
{
    public static OrderedInboxBuilder<TDbContext> AddOrderedInbox<TDbContext>(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<OrderedInboxOptions>? configure = null
    )
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var builder = AddOrderedInbox<TDbContext>(services, configure);
        var registration = GetRegistration<TDbContext>(services);
        if (!registration.Configurations.Contains(configuration))
            registration.Configurations.Add(configuration);

        return builder;
    }

    public static OrderedInboxBuilder<TDbContext> AddOrderedInbox<TDbContext>(
        this IServiceCollection services,
        Action<OrderedInboxOptions>? configure = null
    )
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        var otherContext = services.FirstOrDefault(d =>
            d.ServiceType.IsGenericType
            && d.ServiceType.GetGenericTypeDefinition() == typeof(InboxRegistration<>)
            && d.ServiceType != typeof(InboxRegistration<TDbContext>)
        );
        if (otherContext is not null)
            throw new InvalidOperationException("OrderedInbox uses one application DbContext per service provider.");

        var registration = GetRegistration<TDbContext>(services);
        if (configure is not null)
            registration.Configure.Add(configure);

        services.AddLogging();
        services.TryAddSingleton<InboxMetrics>();
        services.TryAddSingleton<InboxRunningKeys>();
        services.TryAddSingleton<InboxStartupBarrier>();
        // Build once after all modules register. Workers will use this immutable snapshot.
        services.TryAddSingleton<InboxCatalog<TDbContext>>(provider =>
        {
            try
            {
                return new(registration, services, provider);
            }
            catch (OptionsValidationException error)
            {
                provider.GetRequiredService<InboxStartupBarrier>().Fail(error);
                provider
                    .GetRequiredService<ILogger<InboxCatalog<TDbContext>>>()
                    .LogError(
                        error,
                        "OrderedInbox configuration validation failed for {OptionsName}.",
                        error.OptionsName
                    );
                throw;
            }
        });
        // The common schema barrier is registered once; all future subscription workers must await it.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxStartup<TDbContext>>());
        services.TryAddScoped<InboxIntakeStore<TDbContext>>();
        services.TryAddScoped<InboxDiagnostics>();
        services.TryAddScoped<InboxAdministration<TDbContext>>(provider =>
            new(
                provider.GetRequiredService<TDbContext>(),
                provider.GetRequiredService<InboxCatalog<TDbContext>>()
            )
        );
        services.TryAddScoped<InboxProcessor<TDbContext>>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxDispatcher<TDbContext>>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxMetricsReporter<TDbContext>>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxHistoryCleanup<TDbContext>>());
        if (services.All(d => d.ServiceType != typeof(InboxIntake<TDbContext>)))
        {
            services.AddSingleton<InboxIntake<TDbContext>>();
            services.AddSingleton<IHostedService>(static provider =>
                provider.GetRequiredService<InboxIntake<TDbContext>>()
            );
        }
        return new(services, registration);
    }

    private static InboxRegistration<TDbContext> GetRegistration<TDbContext>(IServiceCollection services)
        where TDbContext : DbContext
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(InboxRegistration<TDbContext>));
        if (descriptor?.ImplementationInstance is InboxRegistration<TDbContext> existing)
            return existing;

        var registration = new InboxRegistration<TDbContext>();
        services.AddSingleton(registration);
        return registration;
    }
}

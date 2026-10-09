using System.Collections.Immutable;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;

namespace MonixOne.Inbox.Registration;

internal static class InboxSettingsValidator
{
    // CancellationTokenSource and Task.Delay cannot represent arbitrary TimeSpan values.
    private static readonly TimeSpan _maxTimerInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    internal static void Validate<TDbContext>(
        InboxSettings settings,
        ImmutableArray<InboxHandlerRegistration<TDbContext>> handlers,
        IServiceCollection services,
        IServiceProvider provider,
        List<string> errors
    )
        where TDbContext : DbContext
    {
        if (
            string.IsNullOrEmpty(settings.Schema)
            || settings.Schema.Length > 63
            || !(char.IsAsciiLetterLower(settings.Schema[0]) || settings.Schema[0] == '_')
            || settings.Schema.Any(c => !(char.IsAsciiLetterLower(c) || char.IsAsciiDigit(c) || c == '_'))
            || settings.Schema.StartsWith("pg_", StringComparison.Ordinal)
        )
            errors.Add(
                "Schema must be a lowercase PostgreSQL identifier of at most 63 ASCII characters,"
                    + " without the reserved 'pg_' prefix."
            );

        var cleanup = settings.HistoryCleanup;
        if (cleanup.CompletedRetention <= TimeSpan.Zero || cleanup.DeadLetterRetention <= TimeSpan.Zero)
            errors.Add("HistoryCleanup retention must be positive.");
        CheckInterval(cleanup.Interval, "HistoryCleanup.Interval", errors);
        if (cleanup.BatchSize < 1)
            errors.Add("HistoryCleanup.BatchSize must be positive.");

        if (handlers.IsEmpty)
            errors.Add("Register at least one handler with AddHandler.");

        var available = provider.GetRequiredService<IServiceProviderIsService>();
        var keyed = provider.GetRequiredService<IServiceProviderIsKeyedService>();
        // Inspect registrations without constructing handlers or opening connections.
        if (handlers.Any(h => h.Enabled))
        {
            if (FindService(services, typeof(InboxDiagnostics))?.Lifetime != ServiceLifetime.Scoped)
                errors.Add("InboxDiagnostics must be registered as scoped to isolate business attempts.");
            if (!available.IsService(typeof(TDbContext)))
                errors.Add($"Register the application DbContext '{typeof(TDbContext).Name}' before host startup.");
            else if (FindService(services, typeof(TDbContext))?.Lifetime != ServiceLifetime.Scoped)
                errors.Add(
                    "The application DbContext must be registered as scoped so all operation "
                        + "dependencies share its instance."
                );
        }

        if (FindService(services, typeof(IInboxEnvelopeAdapter))?.Lifetime == ServiceLifetime.Singleton)
            errors.Add("IInboxEnvelopeAdapter must be scoped or transient for operation-scoped dependencies.");

        var consumers = new HashSet<(string? Connection, string Stream, string Durable)>();
        foreach (var handler in handlers)
        {
            CheckId(handler.Id, $"Handler '{handler.Id}'", errors);
            CheckProcessing<TDbContext>(handler.Id, handler.Processing, errors);
            var duplicateIds = handler
                .Subscriptions.GroupBy(s => s.Id, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1);
            foreach (var group in duplicateIds)
                errors.Add(
                    $"Handler '{handler.Id}' has ambiguous subscription ids differing only by case: '{group.Key}'."
                );

            if (handler.Enabled)
            {
                if (!available.IsService(handler.HandlerType))
                    errors.Add($"Register handler dependency '{handler.HandlerType.Name}' for '{handler.Id}' in DI.");
                if (
                    FindService(services, handler.HandlerType)?.Lifetime == ServiceLifetime.Singleton
                )
                    errors.Add($"Handler '{handler.Id}' must be scoped or transient, not singleton.");
                if (handler.Subscriptions.IsEmpty)
                    errors.Add($"Enabled handler '{handler.Id}' requires at least one subscription.");
            }

            foreach (var subscription in handler.Subscriptions)
            {
                var path = $"Subscription '{handler.Id}/{subscription.Id}'";
                CheckId(subscription.Id, path, errors);
                if (!handler.Enabled)
                    continue;

                if (!ValidNatsName(subscription.Stream))
                    errors.Add(
                        $"{path}: Stream is required and cannot contain whitespace, control characters, "
                            + $"'.', '*', '>', '/' or '\\'."
                    );
                if (!ValidNatsName(subscription.DurableName))
                    errors.Add(
                        $"{path}: DurableName is required and cannot contain whitespace, control "
                            + $"characters, '.', '*', '>', '/' or '\\'."
                    );
                if (!ValidSubject(subscription.Subject))
                    errors.Add(
                        $"{path}: Subject must have nonempty tokens; '*' is a whole token and '>' is the "
                            + $"final whole token."
                    );
                if (subscription.BatchSize < 1)
                    errors.Add($"{path}: BatchSize must be positive.");
                if (subscription.ConnectionName is not null && string.IsNullOrWhiteSpace(subscription.ConnectionName))
                    errors.Add(
                        $"{path}: ConnectionName must be omitted for the default connection or contain a "
                            + $"nonempty DI key."
                    );

                var hasConnection = subscription.ConnectionName is null
                    ? available.IsService(typeof(INatsConnection))
                    : keyed.IsKeyedService(typeof(INatsConnection), subscription.ConnectionName);
                var connectionDescription = subscription.ConnectionName is null
                    ? "the default"
                    : $"keyed '{subscription.ConnectionName}'";
                if (!hasConnection)
                    errors.Add($"{path}: Register " + $"{connectionDescription} INatsConnection in DI.");
                else if (
                    FindService(services, typeof(INatsConnection), subscription.ConnectionName)?.Lifetime
                    != ServiceLifetime.Singleton
                )
                    errors.Add(
                        $"{path}: INatsConnection must be singleton; the application DI container owns its lifetime."
                    );

                if (!consumers.Add((subscription.ConnectionName, subscription.Stream, subscription.DurableName)))
                    errors.Add(
                        $"{path}: This connection/stream/durable consumer is already assigned to another "
                            + $"subscription. Use independent durable consumers."
                    );
            }
        }
    }

    private static ServiceDescriptor? FindService(IServiceCollection services, Type serviceType, string? key = null) =>
        services.LastOrDefault(d =>
            d.ServiceType == serviceType
            && (key is null ? !d.IsKeyedService : d.IsKeyedService && Equals(d.ServiceKey, key))
        );

    private static void CheckId(string id, string path, List<string> errors)
    {
        if (
            string.IsNullOrEmpty(id)
            || id.Length > 128
            || !char.IsAsciiLetterOrDigit(id[0])
            || id.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.'))
        )
            errors.Add(
                $"{path}: Id must start with an ASCII letter or digit and contain at most 128 "
                    + $"letters, digits, '.', '_' or '-'."
            );
    }

    private static void CheckProcessing<TDbContext>(string id, InboxProcessingSettings settings, List<string> errors)
        where TDbContext : DbContext
    {
        var path = $"Handler '{id}'";
        if (settings.MaxParallelObjects < 1)
            errors.Add($"{path}: MaxParallelObjects must be positive.");
        CheckInterval(settings.HandlerTimeout, $"{path}: HandlerTimeout", errors);
        CheckInterval(settings.PollInterval, $"{path}: PollInterval", errors);
        CheckInterval(settings.StartupReorderWindow, $"{path}: StartupReorderWindow", errors, allowZero: true);
        CheckInterval(settings.GapTimeout, $"{path}: GapTimeout", errors, allowZero: true);
        if (settings.IntakeConcurrency < 1)
            errors.Add($"{path}: IntakeConcurrency must be positive.");
        if (settings.Retry.MaxAttempts < 1)
            errors.Add($"{path}: Retry.MaxAttempts must include at least the first attempt.");
        CheckInterval(settings.Retry.InitialDelay, $"{path}: Retry.InitialDelay", errors);
        CheckInterval(settings.Retry.MaxDelay, $"{path}: Retry.MaxDelay", errors);
        if (settings.Retry.MaxDelay < settings.Retry.InitialDelay)
            errors.Add($"{path}: Retry.MaxDelay must be greater than or equal to InitialDelay.");
    }

    private static void CheckInterval(TimeSpan value, string path, List<string> errors, bool allowZero = false)
    {
        if (value < (allowZero ? TimeSpan.Zero : TimeSpan.FromMilliseconds(1)) || value > _maxTimerInterval)
            errors.Add($"{path} must be between 1 ms and {_maxTimerInterval}.");
    }

    private static bool ValidNatsName(string value) =>
        !string.IsNullOrEmpty(value)
        && !value.Any(c => char.IsWhiteSpace(c) || char.IsControl(c) || c is '.' or '*' or '>' or '/' or '\\');

    private static bool ValidSubject(string subject)
    {
        if (string.IsNullOrEmpty(subject) || subject.Any(c => char.IsWhiteSpace(c) || char.IsControl(c)))
            return false;

        var tokens = subject.Split('.');
        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (
                token.Length == 0
                || (token.Contains('*') && token != "*")
                || (token.Contains('>') && (token != ">" || i != tokens.Length - 1))
            )
                return false;
        }

        return true;
    }
}

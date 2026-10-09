using System.Collections.Immutable;
using Microsoft.EntityFrameworkCore;

namespace MonixOne.Inbox.Registration;

internal sealed record InboxSettings(string Schema, bool AutoMigrate, InboxCleanupSettings HistoryCleanup);

internal sealed record InboxCleanupSettings(
    bool Enabled, TimeSpan CompletedRetention, TimeSpan DeadLetterRetention, TimeSpan Interval, int BatchSize)
{
    internal static InboxCleanupSettings Defaults { get; } =
        new(true, TimeSpan.FromDays(30), TimeSpan.FromDays(90), TimeSpan.FromHours(1), 500);

    internal InboxCleanupSettings Apply(InboxHistoryCleanupOptions? o) => o is null ? this : this with
    {
        Enabled = o.Enabled ?? Enabled,
        CompletedRetention = o.CompletedRetention ?? CompletedRetention,
        DeadLetterRetention = o.DeadLetterRetention ?? DeadLetterRetention,
        Interval = o.Interval ?? Interval,
        BatchSize = o.BatchSize ?? BatchSize,
    };
}

internal sealed record InboxProcessingSettings(
    int MaxParallelObjects,
    TimeSpan HandlerTimeout,
    TimeSpan PollInterval,
    TimeSpan StartupReorderWindow,
    TimeSpan GapTimeout,
    int IntakeConcurrency,
    InboxRetrySettings Retry,
    Func<Exception, InboxResult>? ClassifyException)
{
    internal static InboxProcessingSettings Defaults { get; } = new(
        4, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(5), 4, new(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)), null);

    internal InboxProcessingSettings Apply(InboxProcessingOptions? o) => o is null ? this : this with
    {
        MaxParallelObjects = o.MaxParallelObjects ?? MaxParallelObjects,
        HandlerTimeout = o.HandlerTimeout ?? HandlerTimeout,
        PollInterval = o.PollInterval ?? PollInterval,
        StartupReorderWindow = o.StartupReorderWindow ?? StartupReorderWindow,
        GapTimeout = o.GapTimeout ?? GapTimeout,
        IntakeConcurrency = o.IntakeConcurrency ?? IntakeConcurrency,
        Retry = Retry with
        {
            MaxAttempts = o.Retry?.MaxAttempts ?? Retry.MaxAttempts,
            InitialDelay = o.Retry?.InitialDelay ?? Retry.InitialDelay,
            MaxDelay = o.Retry?.MaxDelay ?? Retry.MaxDelay,
        },
        ClassifyException = o.ClassifyException ?? ClassifyException,
    };
}

internal sealed record InboxRetrySettings(int MaxAttempts, TimeSpan InitialDelay, TimeSpan MaxDelay);

/// <summary>
/// Эффективные транспортные параметры подписки; бизнес-настройки принадлежат handler.
/// </summary>
/// <param name="Id">
/// Стабильный subscription_id для конфигурации и диагностики, не часть sequence key.
/// </param>
/// <param name="ConnectionName">
/// DI key singleton INatsConnection либо null для default-соединения.
/// </param>
/// <param name="Stream">
/// Существующий JetStream stream с совместимой retention policy.
/// </param>
/// <param name="Subject">
/// Фильтр NATS subject; все номера исходного счётчика должны поступать handler.
/// </param>
/// <param name="DurableName">
/// Общий durable реплик данной подписки; другой handler имеет отдельный durable.
/// </param>
/// <param name="BatchSize">
/// Верхняя граница сообщений одного fetch; default: 32.
/// </param>
internal sealed record InboxSubscriptionSettings(
    string Id,
    string? ConnectionName,
    string Stream,
    string Subject,
    string DurableName,
    int BatchSize
)
{
    /// <summary>
    /// Накладывает транспортные overrides по id без сброса неуказанных параметров.
    /// </summary>
    internal InboxSubscriptionSettings Apply(InboxSubscriptionOptions options) =>
        this with
        {
            ConnectionName = options.ConnectionName ?? ConnectionName,
            Stream = options.Stream ?? Stream,
            Subject = options.Subject ?? Subject,
            DurableName = options.DurableName ?? DurableName,
            BatchSize = options.BatchSize ?? BatchSize,
        };
}

/// <summary>
/// Готовая регистрация handler; зависимости разрешаются только внутри операции.
/// </summary>
/// <param name="Id">
/// Стабильный handler_id, разделяющий inbox и cursor логических потребителей.
/// </param>
/// <param name="HandlerType">
/// Тип бизнес-обработчика для DI и диагностики.
/// </param>
/// <param name="Resolve">
/// Фабрика обработчика из scope текущей бизнес-попытки.
/// </param>
/// <param name="Enabled">
/// Запускать handler; при false транспортные и бизнес-зависимости не создаются.
/// </param>
/// <param name="Processing">
/// Неизменяемые эффективные параметры обработки и envelope.
/// </param>
/// <param name="Subscriptions">
/// Проверенные подписки одного handler с общими ключами последовательности.
/// </param>
internal sealed record InboxHandlerRegistration<TDbContext>(
    string Id,
    Type HandlerType,
    Func<IServiceProvider, IInboxHandler<TDbContext>> Resolve,
    bool Enabled,
    InboxProcessingSettings Processing,
    ImmutableArray<InboxSubscriptionSettings> Subscriptions
)
    where TDbContext : DbContext;

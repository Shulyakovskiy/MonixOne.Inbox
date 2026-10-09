using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace MonixOne.Inbox.Registration;

/// <summary>
/// Настройки, собранные всеми модулями до старта host.
/// Catalog применяет их по приоритетам и создаёт единый неизменяемый снимок.
/// </summary>
internal sealed class InboxRegistration<TDbContext>
    where TDbContext : DbContext
{
    /// <summary>
    /// Секции внешней конфигурации в порядке регистрации; привязка выполняется при старте.
    /// </summary>
    internal List<IConfiguration> Configurations { get; } = [];

    /// <summary>
    /// Явные общие C# overrides, применяемые после внешней конфигурации.
    /// </summary>
    internal List<Action<OrderedInboxOptions>> Configure { get; } = [];

    /// <summary>
    /// Стабильные handler ids, бизнес-фабрики и локальные C# настройки.
    /// </summary>
    internal List<HandlerDescriptor<TDbContext>> Handlers { get; } = [];

    /// <summary>
    /// Дополнительные транспортные overrides по handler_id/subscription_id.
    /// </summary>
    internal List<SubscriptionDescriptor> Subscriptions { get; } = [];
}

/// <summary>
/// Регистрация C# handler до объединения с настройками конфигурации.
/// </summary>
/// <param name="Id">
/// Стабильный id логического потребителя, общий для всех его реплик.
/// </param>
/// <param name="HandlerType">
/// Тип обработчика.
/// </param>
/// <param name="Resolve">
/// Разрешает бизнес-зависимости из scope текущей попытки, не из singleton worker.
/// </param>
/// <param name="Configure">
/// Локальные C# overrides поверх общих и handler-настроек из конфигурации.
/// </param>
internal sealed record HandlerDescriptor<TDbContext>(
    string Id,
    Type HandlerType,
    Func<IServiceProvider, IInboxHandler<TDbContext>> Resolve,
    Action<InboxHandlerOptions>? Configure
)
    where TDbContext : DbContext;

/// <summary>
/// Транспортные overrides, добавляемые отдельным модулем через AddSubscription.
/// </summary>
/// <param name="HandlerId">
/// Id зарегистрированного handler; порядок регистрации модулей не важен.
/// </param>
/// <param name="Id">
/// Id подписки, по которому объединяются частичные настройки.
/// </param>
/// <param name="Configure">
/// Последний уровень транспортных overrides; бизнес-параметры не меняет.
/// </param>
internal sealed record SubscriptionDescriptor(string HandlerId, string Id, Action<InboxSubscriptionOptions> Configure);

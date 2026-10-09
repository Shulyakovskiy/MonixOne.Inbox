namespace MonixOne.Inbox;

/// <summary>
/// Настройки пакета. Nullable-значения означают наследование либо встроенный default.
/// При старте host создаётся неизменяемый снимок; изменения требуют перезапуска.
/// </summary>
public sealed class OrderedInboxOptions
{
    /// <summary>
    /// PostgreSQL schema для общих таблиц пакета. По умолчанию ordered_inbox.
    /// Не относится к schema бизнес-таблиц приложения.
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// Устанавливать недостающие миграции при старте host. По умолчанию true.
    /// При false схема только проверяется и должна быть установлена заранее.
    /// </summary>
    public bool? AutoMigrate { get; set; }

    /// <summary>Глобальная очистка истории общих таблиц пакета.</summary>
    public InboxHistoryCleanupOptions HistoryCleanup { get; set; } = new();

    /// <summary>
    /// Общие настройки, наследуемые обработчиками.
    /// Явные настройки конкретного handler имеют приоритет над Defaults.
    /// </summary>
    public InboxProcessingOptions Defaults { get; set; } = new();

    /// <summary>
    /// Настройки по стабильному handler_id. Каждый handler отдельно хранит cursor и inbox.
    /// C#-обработчик для каждого id необходимо зарегистрировать через AddHandler.
    /// </summary>
    public Dictionary<string, InboxHandlerOptions> Handlers { get; set; } = new(StringComparer.Ordinal);
}

/// <summary>
/// Общие параметры обработки. null наследует значение из предыдущего уровня настройки.
/// Бизнес-параметры применяются dispatcher; приём в БД работает независимо от обработчика.
/// </summary>
public class InboxProcessingOptions
{
    /// <summary>
    /// Число объектов, обрабатываемых параллельно одним handler на одной реплике. Default: 4, минимум: 1.
    /// Внутри процесса один callback на ключ; между репликами результат согласуется по version.
    /// </summary>
    public int? MaxParallelObjects { get; set; }

    /// <summary>
    /// Лимит времени одной бизнес-попытки. Default: 30 секунд.
    /// Обработчик должен соблюдать переданный CancellationToken; это не таймаут NATS fetch.
    /// При игнорировании токена пакет ждёт завершения callback, не запуская параллельный повтор.
    /// </summary>
    public TimeSpan? HandlerTimeout { get; set; }

    /// <summary>
    /// Пауза dispatcher при отсутствии доступной работы. Default: 250 ms.
    /// Задержка бизнес-retry задаётся отдельно; NATS intake не использует этот интервал.
    /// </summary>
    public TimeSpan? PollInterval { get; set; }

    /// <summary>Окно перестановки первого номера нового ключа. Default: 1 секунда; допускается 0.</summary>
    public TimeSpan? StartupReorderWindow { get; set; }

    /// <summary>Ожидание отсутствующего следующего номера. Default: 5 секунд; допускается 0.</summary>
    public TimeSpan? GapTimeout { get; set; }

    /// <summary>Параллельные сохранения на подписку и реплику. Default: 4.</summary>
    public int? IntakeConcurrency { get; set; }

    /// <summary>
    /// Повторные бизнес-попытки уже сохранённого inbox.
    /// Переподключения инфраструктуры и повторные доставки NATS в этот лимит не входят.
    /// </summary>
    public InboxRetryOptions Retry { get; set; } = new();

    /// <summary>
    /// Быстрая функция классификации бизнес-исключений; задаётся только кодом.
    /// Возвращает Retry либо Reject. Default: null, все бизнес-исключения приводят к Retry.
    /// null наследует предыдущее правило. Timeout, отмена host и потеря соединения не классифицируются.
    /// Ошибка классификатора или другой результат сохраняются в диагностике с fallback Retry.
    /// </summary>
    public Func<Exception, InboxResult>? ClassifyException { get; set; }
}

/// <summary>
/// Настройки одного логического handler и его транспортных подписок.
/// </summary>
public sealed class InboxHandlerOptions : InboxProcessingOptions
{
    /// <summary>
    /// Запускать приём и обработку handler. Default: true.
    /// При false не нужны активные подписки, бизнес-зависимости и NATS-соединение.
    /// </summary>
    public bool? Enabled { get; set; }

    /// <summary>
    /// Подписки по стабильному subscription_id. Нужна хотя бы одна для активного handler.
    /// Несколько подписок одного handler используют общий cursor по ключу производителя.
    /// </summary>
    public Dictionary<string, InboxSubscriptionOptions> Subscriptions { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Добавляет или обновляет подписку с обязательными транспортными параметрами.
    /// Сохраняет ранее настроенный BatchSize и неуказанный ConnectionName.
    /// </summary>
    /// <param name="subscriptionId">
    /// Стабильный id подписки для конфигурации и диагностики.
    /// </param>
    /// <param name="stream">
    /// Имя заранее созданного JetStream stream.
    /// </param>
    /// <param name="subject">
    /// Фильтр NATS subject; разрешены wildcard tokens * и финальный &gt;.
    /// </param>
    /// <param name="durableName">
    /// Общий durable для реплик одного handler; отдельный для другого handler.
    /// </param>
    /// <param name="connectionName">
    /// DI key INatsConnection либо null для default-соединения.
    /// </param>
    public InboxHandlerOptions Subscribe(
        string subscriptionId,
        string stream,
        string subject,
        string durableName,
        string? connectionName = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionId);
        var subscription = Subscriptions.GetValueOrDefault(subscriptionId) ?? new InboxSubscriptionOptions();
        subscription.Stream = stream;
        subscription.Subject = subject;
        subscription.DurableName = durableName;
        subscription.ConnectionName = connectionName ?? subscription.ConnectionName;
        Subscriptions[subscriptionId] = subscription;
        return this;
    }
}

/// <summary>
/// Транспортные параметры одной подписки; null сохраняет унаследованное значение.
/// </summary>
public sealed class InboxSubscriptionOptions
{
    /// <summary>
    /// DI key singleton INatsConnection. Без ключа используется default-регистрация.
    /// Соединением и его lifetime управляет приложение.
    /// </summary>
    public string? ConnectionName { get; set; }

    /// <summary>
    /// Имя существующего JetStream stream. Обязательно для активной подписки.
    /// Пакет проверяет retention и создаёт только consumer, а не сам stream.
    /// </summary>
    public string? Stream { get; set; }

    /// <summary>
    /// Фильтр событий подписки. Обязателен; поддерживает * и финальный &gt; по токенам.
    /// Все номера исходной sequence должны попадать в подписки соответствующего handler.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Имя durable pull consumer. Обязательно; сохраняется между рестартами и репликами.
    /// Разные handlers одного subject используют разные durables.
    /// </summary>
    public string? DurableName { get; set; }

    /// <summary>
    /// Максимальное число сообщений одного fetch. Default: 32, минимум: 1.
    /// Следующий batch запрашивается после сохранения текущего; это не бизнес-параллельность.
    /// </summary>
    public int? BatchSize { get; set; }
}

/// <summary>
/// Настройки бизнес-retry; null наследует значение из defaults или handler.
/// </summary>
public sealed class InboxRetryOptions
{
    /// <summary>
    /// Максимум бизнес-попыток, включая первую. Default: 5, минимум: 1.
    /// Ожидание sequence и инфраструктурные переподключения попытками не считаются.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// Начальная задержка автоматического бизнес-retry. Default: 1 секунда.
    /// </summary>
    public TimeSpan? InitialDelay { get; set; }

    /// <summary>
    /// Верхняя граница растущей задержки retry. Default: 1 минута.
    /// Должна быть не меньше InitialDelay. Явный retryAfter ограничен диапазоном 1 мс — MaxDelay.
    /// </summary>
    public TimeSpan? MaxDelay { get; set; }
}

/// <summary>Пакетная очистка терминальной истории. Cursor сохраняется без TTL.</summary>
public sealed class InboxHistoryCleanupOptions
{
    /// <summary>Запускать фоновую очистку. Default: true.</summary>
    public bool? Enabled { get; set; }
    /// <summary>Хранить processed/skipped. Default: 30 дней.</summary>
    public TimeSpan? CompletedRetention { get; set; }
    /// <summary>Хранить DLQ и необходимую исходную историю. Default: 90 дней.</summary>
    public TimeSpan? DeadLetterRetention { get; set; }
    /// <summary>Период очистки. Default: 1 час.</summary>
    public TimeSpan? Interval { get; set; }
    /// <summary>Максимум событий или DLQ в одной короткой транзакции. Default: 500.</summary>
    public int? BatchSize { get; set; }
}

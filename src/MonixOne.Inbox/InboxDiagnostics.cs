using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace MonixOne.Inbox;

/// <summary>
/// Scoped-журнал текущей бизнес-попытки. Доступен handler и его зависимостям через DI.
/// Записи фиксируются вместе с итогом попытки, в том числе после бизнес-rollback.
/// До commit записи недолговечны; crash, отмена host или потеря транзакции их не сохраняют.
/// </summary>
public sealed class InboxDiagnostics(ILogger<InboxDiagnostics> logger) : IDisposable
{
    // Fixed bounds include one reserved truncation entry. No extra tuning is required from the application.
    internal const int MaxEntries = 128;
    internal const int MaxBytes = 64 * 1024;

    // Leave room for the diagnostic entry/array around an envelope accepted at the standard JSON depth.
    private static readonly JsonSerializerOptions _jsonOptions = new() { MaxDepth = 128 };
    private readonly object _gate = new();
    private readonly List<JsonElement> _entries = [];
    private Guid? _attemptId;
    private int _bytes = 2;
    private long _dropped;
    private bool _disposed;

    /// <summary>
    /// Id активной попытки для корреляции; null вне вызова handler.
    /// </summary>
    public Guid? AttemptId
    {
        get
        {
            lock (_gate)
                return _attemptId;
        }
    }

    /// <summary>
    /// Добавляет структурированную диагностику и передаёт её в обычный ILogger.
    /// JSON копируется сразу; данные сохраняются после освобождения исходного JsonDocument.
    /// Превышение лимита 128 записей / 64 KiB отмечается diagnostics.truncated и не меняет бизнес-результат.
    /// </summary>
    /// <param name="code">
    /// Непустой машинный код диагностического сообщения.
    /// </param>
    /// <param name="message">
    /// Непустое пояснение. Не передавайте credentials и secrets.
    /// </param>
    /// <param name="details">
    /// Необязательные JSON-данные. Default: null. Приложение выбирает безопасные для журнала поля.
    /// </param>
    /// <param name="level">
    /// Уровень ILogger и долговечной записи. Default: Information; None недопустим.
    /// </param>
    /// <param name="exception">
    /// Необязательная ошибка для ILogger и полного Exception.ToString() в журнале. Default: null.
    /// </param>
    public void Write(
        string code,
        string message,
        JsonElement? details = null,
        LogLevel level = LogLevel.Information,
        Exception? exception = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        if (!Enum.IsDefined(level) || level == LogLevel.None)
            throw new ArgumentOutOfRangeException(nameof(level));
        details = InboxDiagnosticJson.Copy(details);
        Guid attemptId;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            attemptId = _attemptId ?? throw new InvalidOperationException("No active OrderedInbox business attempt.");
            var bytes = JsonSerializer.SerializeToUtf8Bytes(
                new
                {
                    timestamp = DateTimeOffset.UtcNow,
                    level = level.ToString(),
                    code,
                    message,
                    details,
                    exception = exception?.ToString(),
                },
                _jsonOptions
            );
            if (_entries.Count >= MaxEntries - 1 || bytes.Length > MaxBytes - 512 - _bytes)
                _dropped++;
            else
            {
                using var json = JsonDocument.Parse(bytes, new JsonDocumentOptions { MaxDepth = 128 });
                _entries.Add(json.RootElement.Clone());
                _bytes += bytes.Length + 1;
            }
        }
        // ILogger filtering never prevents collection in the durable attempt history.
        if (!logger.IsEnabled(level))
            return;
        logger.Log(
            level,
            exception,
            "{DiagnosticCode}: {DiagnosticMessage}; details {DiagnosticDetails}, attempt {AttemptId}.",
            code,
            message,
            details?.GetRawText(),
            attemptId
        );
    }

    internal void Begin(Guid attemptId)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_attemptId is not null)
                throw new InvalidOperationException(
                    "An OrderedInbox business attempt is already active in this scope."
                );
            _attemptId = attemptId;
            _entries.Clear();
            _bytes = 2;
            _dropped = 0;
        }
    }

    internal string Complete()
    {
        lock (_gate)
        {
            _attemptId = null;
            if (_dropped > 0)
                _entries.Add(
                    JsonSerializer.SerializeToElement(
                        new
                        {
                            timestamp = DateTimeOffset.UtcNow,
                            level = nameof(LogLevel.Warning),
                            code = "diagnostics.truncated",
                            message = "Attempt diagnostics exceeded the fixed entry/byte limit.",
                            dropped_entries = _dropped,
                        }
                    )
                );
            var json = JsonSerializer.Serialize(_entries, _jsonOptions);
            _entries.Clear();
            return json;
        }
    }

    /// <summary>
    /// Завершает сбор и освобождает буфер scope. Поздние записи в завершённую попытку недопустимы.
    /// </summary>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _attemptId = null;
            _entries.Clear();
        }
    }
}

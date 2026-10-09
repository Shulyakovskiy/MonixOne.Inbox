using System.Text.Json;

namespace MonixOne.Inbox;

/// <summary>
/// Решение бизнес-обработчика о завершении текущего номера sequence.
/// </summary>
public enum InboxResultKind
{
    /// <summary>
    /// Бизнес-изменения готовы к commit вместе с cursor.
    /// </summary>
    Applied,

    /// <summary>
    /// Закрыть номер без бизнес-изменений.
    /// </summary>
    Ignore,

    /// <summary>
    /// Откатить бизнес-изменения и отложить новую попытку.
    /// </summary>
    Retry,

    /// <summary>
    /// Окончательная ошибка; DLQ и закрытие номера без бизнес-изменений.
    /// </summary>
    Reject,
}

/// <summary>
/// Неизменяемый результат одной бизнес-попытки и её диагностические данные.
/// </summary>
public sealed record InboxResult
{
    private InboxResult(
        InboxResultKind kind,
        string code,
        string? reason = null,
        TimeSpan? retryAfter = null,
        JsonElement? details = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        if (code.Contains('\0') || reason?.Contains('\0') == true)
            throw new ArgumentException("Result code and reason cannot contain NUL.");
        Kind = kind;
        Code = code;
        Reason = reason;
        RetryAfter = retryAfter;
        // Diagnostics must survive disposal of the handler's JsonDocument.
        Details = InboxDiagnosticJson.Copy(details);
    }

    /// <summary>
    /// Решение о commit, пропуске, повторе или окончательной ошибке.
    /// </summary>
    public InboxResultKind Kind { get; }

    /// <summary>
    /// Непустой машинный код результата для журнала и диагностики.
    /// </summary>
    public string Code { get; }

    /// <summary>
    /// Причина пропуска, повтора или отклонения; для Applied не нужна.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// Явная положительная задержка Retry; null использует автоматическую retry-политику.
    /// </summary>
    public TimeSpan? RetryAfter { get; }

    /// <summary>
    /// Структурированная диагностика; копия JSON сохраняется после освобождения исходного документа.
    /// Лимит: 64 KiB / глубина 32; превышение заменяется безопасным fallback без изменения Kind.
    /// </summary>
    public JsonElement? Details { get; }

    /// <summary>
    /// Успешное применение; пакет фиксирует бизнес-транзакцию и закрывает номер.
    /// </summary>
    public static InboxResult Applied { get; } = new(InboxResultKind.Applied, "applied");

    /// <summary>
    /// Пропуск с причиной; бизнес-транзакция откатывается, номер закрывается.
    /// </summary>
    public static InboxResult Ignore(string reason, string code = "ignored", JsonElement? details = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new(InboxResultKind.Ignore, code, reason, details: details);
    }

    /// <summary>
    /// Повтор с причиной и необязательной явной задержкой; бизнес-транзакция откатывается.
    /// </summary>
    public static InboxResult Retry(
        string reason,
        TimeSpan? retryAfter = null,
        string code = "retry_requested",
        JsonElement? details = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        if (retryAfter <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retryAfter), "Retry delay must be positive.");

        return new(InboxResultKind.Retry, code, reason, retryAfter, details);
    }

    /// <summary>
    /// Окончательная ошибка с причиной; номер закрывается, поток продолжает работу.
    /// </summary>
    public static InboxResult Reject(string reason, string code = "rejected", JsonElement? details = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new(InboxResultKind.Reject, code, reason, details: details);
    }
}

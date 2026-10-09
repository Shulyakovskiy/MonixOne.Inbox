using System.Text.Json;

namespace MonixOne.Inbox;

/// <summary>
/// Согласованный снимок события и страница его долговечной истории.
/// JSON содержит имена полей в snake_case; raw_payload представлен в Base64.
/// </summary>
/// <param name="Inbox">
/// Исходное событие и текущий статус, включая run_id и бюджет попыток.
/// Null для некорректного сообщения, которое не попало в inbox.
/// </param>
/// <param name="ConsumerState">
/// Текущий nullable cursor и version по ключу события.
/// Null, если у сообщения нет соответствующей записи inbox.
/// </param>
/// <param name="DeadLetter">
/// Выбранная DLQ-запись при чтении по её id.
/// При чтении по inbox_id DLQ-записи доступны через Entries.
/// </param>
/// <param name="Entries">
/// Записи журнала по возрастанию id с полными связанными попытками и DLQ.
/// История включает все run_id, а не только текущий прогон.
/// </param>
/// <param name="NextJournalId">
/// Последний возвращённый id для следующей страницы.
/// Null, если в текущем снимке больше записей нет.
/// </param>
public sealed record InboxHistoryPage(
    JsonElement? Inbox,
    JsonElement? ConsumerState,
    JsonElement? DeadLetter,
    IReadOnlyList<InboxHistoryEntry> Entries,
    long? NextJournalId
);

/// <summary>
/// Запись истории с долговечными доказательствами соответствующего решения.
/// JSON-значения независимы от DbContext и не требуют Dispose.
/// </summary>
/// <param name="JournalId">
/// Монотонный id записи журнала, используемый для пагинации.
/// Пропуски id после отката транзакций допустимы.
/// </param>
/// <param name="Journal">
/// Решение, время, worker, ссылки на inbox/attempt/DLQ и структурированные детали.
/// </param>
/// <param name="Attempt">
/// Полная зафиксированная попытка с diagnostics, исключением и настройками.
/// Null для решений без бизнес-вызова.
/// </param>
/// <param name="DeadLetter">
/// Полная DLQ-запись, связанная с решением; null, если такой связи нет.
/// </param>
public sealed record InboxHistoryEntry(
    long JournalId,
    JsonElement Journal,
    JsonElement? Attempt,
    JsonElement? DeadLetter
);


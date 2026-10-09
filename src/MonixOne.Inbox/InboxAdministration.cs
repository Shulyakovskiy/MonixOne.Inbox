using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using MonixOne.Inbox.Intake;
using MonixOne.Inbox.Processing;
using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox;

/// <summary>
/// Scoped API чтения истории.
/// Используйте отдельный DI scope вне бизнес-транзакции обработчика.
/// Миграции выполняются при запуске host; API не создаёт таблицы неявно.
/// </summary>
public sealed class InboxAdministration<TDbContext>
    where TDbContext : DbContext
{
    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        MaxDepth = 256,
    };
    private readonly TDbContext _context;
    private readonly string _schema;

    internal InboxAdministration(
        TDbContext context,
        InboxCatalog<TDbContext> catalog
    )
    {
        _context = context;
        _schema = catalog.Settings.Schema;
    }

    /// <summary>
    /// Читает событие, состояние sequence и страницу истории всех его прогонов.
    /// Все запросы страницы выполняются в одной RepeatableRead-транзакции.
    /// </summary>
    /// <param name="inboxId">
    /// Id сохранённого события. Для неизвестного id возвращается null.
    /// </param>
    /// <param name="afterJournalId">
    /// Исключительная нижняя граница id журнала; 0 для первой страницы.
    /// Для продолжения передайте NextJournalId предыдущей страницы.
    /// </param>
    /// <param name="pageSize">
    /// Количество записей журнала, от 1 до 200; по умолчанию 50.
    /// Связанные попытки и DLQ загружаются пакетными запросами.
    /// </param>
    /// <param name="cancellationToken">
    /// Отмена чтения и ожидания подключения к БД.
    /// </param>
    public Task<InboxHistoryPage?> ReadHistoryAsync(
        Guid inboxId,
        long afterJournalId = 0,
        int pageSize = 50,
        CancellationToken cancellationToken = default
    ) => ReadAsync(inboxId, null, afterJournalId, pageSize, cancellationToken);

    /// <summary>
    /// Читает выбранную DLQ-запись и историю связанного оригинала.
    /// Для invalid_message история доступна без записи inbox.
    /// При конфликте journal содержит ссылки на затронутые оригиналы;
    /// их полную историю можно отдельно прочитать через ReadHistoryAsync.
    /// </summary>
    /// <param name="deadLetterId">
    /// Id DLQ-записи. Для неизвестного id возвращается null.
    /// </param>
    /// <param name="afterJournalId">
    /// Исключительная нижняя граница id журнала; 0 для первой страницы.
    /// </param>
    /// <param name="pageSize">
    /// Количество записей журнала, от 1 до 200; по умолчанию 50.
    /// </param>
    /// <param name="cancellationToken">
    /// Отмена чтения и ожидания подключения к БД.
    /// </param>
    public Task<InboxHistoryPage?> ReadDeadLetterAsync(
        Guid deadLetterId,
        long afterJournalId = 0,
        int pageSize = 50,
        CancellationToken cancellationToken = default
    ) => ReadAsync(null, deadLetterId, afterJournalId, pageSize, cancellationToken);

    private Task<InboxHistoryPage?> ReadAsync(
        Guid? inboxId,
        Guid? deadLetterId,
        long afterJournalId,
        int pageSize,
        CancellationToken token
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(afterJournalId);
        if (pageSize is < 1 or > 200)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be between 1 and 200.");
        EnsureOwnTransaction();
        return _context
            .Database.CreateExecutionStrategy()
            .ExecuteAsync<InboxHistoryPage?>(
                async cancellation =>
                {
                    await using var transaction = await _context.Database.BeginTransactionAsync(
                        IsolationLevel.RepeatableRead,
                        cancellation
                    );
                    await using var data = Connection(transaction);
                    var letters = data.GetTable<InboxDlqRow>().SchemaName(_schema);
                    InboxDlqRow? letter = null;
                    if (deadLetterId is not null)
                    {
                        letter = await letters.SingleOrDefaultAsyncLinqToDB(x => x.Id == deadLetterId, cancellation);
                        if (letter is null)
                            return null;
                    }
                    var currentInboxId = letter is null ? inboxId : letter.InboxId ?? letter.RelatedInboxId;
                    var row = currentInboxId is null
                        ? null
                        : await data.GetTable<InboxRow>()
                            .SchemaName(_schema)
                            .SingleOrDefaultAsyncLinqToDB(x => x.Id == currentInboxId, cancellation);
                    if (row is null && letter is null)
                        return null;
                    var state = row is null
                        ? null
                        : await data.GetTable<InboxStateRow>()
                            .SchemaName(_schema)
                            .SingleOrDefaultAsyncLinqToDB(
                                x =>
                                    x.HandlerId == row.HandlerId
                                    && x.Producer == row.Producer
                                    && x.SequenceScope == row.SequenceScope
                                    && x.ObjectKey == row.ObjectKey,
                                cancellation
                            );
                    var rows = await data.GetTable<InboxJournalRow>()
                        .SchemaName(_schema)
                        .Where(x =>
                            x.Id > afterJournalId
                            && (
                                currentInboxId != null && x.InboxId == currentInboxId
                                || deadLetterId != null && x.DlqId == deadLetterId
                            )
                        )
                        .OrderBy(x => x.Id)
                        .Take(pageSize + 1)
                        .ToListAsyncLinqToDB(cancellation);
                    var more = rows.Count > pageSize;
                    if (more)
                        rows.RemoveAt(pageSize);
                    var attemptIds = rows.Where(x => x.AttemptId is not null)
                        .Select(x => x.AttemptId!.Value)
                        .Distinct()
                        .ToArray();
                    var dlqIds = rows.Where(x => x.DlqId is not null).Select(x => x.DlqId!.Value).Distinct().ToArray();
                    var attempts =
                        attemptIds.Length == 0
                            ? []
                            : await data.GetTable<InboxAttemptRow>()
                                .SchemaName(_schema)
                                .Where(x => Enumerable.Contains(attemptIds, x.AttemptId))
                                .ToListAsyncLinqToDB(cancellation);
                    var dlqs =
                        dlqIds.Length == 0
                            ? []
                            : await letters
                                .Where(x => Enumerable.Contains(dlqIds, x.Id))
                                .ToListAsyncLinqToDB(cancellation);
                    var attemptSnapshots = attempts.ToDictionary(x => x.AttemptId, x => Snapshot(x)!.Value);
                    var dlqSnapshots = dlqs.ToDictionary(x => x.Id, x => Snapshot(x)!.Value);
                    var entries = rows.Select(x => new InboxHistoryEntry(
                            x.Id,
                            Snapshot(x)!.Value,
                            x.AttemptId is { } a && attemptSnapshots.TryGetValue(a, out var attempt) ? attempt : null,
                            x.DlqId is { } d && dlqSnapshots.TryGetValue(d, out var dlq) ? dlq : null
                        ))
                        .ToArray();
                    var page = new InboxHistoryPage(
                        Snapshot(row),
                        Snapshot(state),
                        Snapshot(letter),
                        entries,
                        more ? rows[^1].Id : null
                    );
                    await transaction.CommitAsync(cancellation);
                    return page;
                },
                token
            );
    }

    private DataConnection Connection(IDbContextTransaction transaction)
    {
        var data = _context.CreateLinqToDBConnection(transaction);
        if (_context.Database.GetCommandTimeout() is { } timeout)
            data.CommandTimeout = timeout;
        return data;
    }

    private void EnsureOwnTransaction()
    {
        if (_context.Database.CurrentTransaction is not null)
            throw new InvalidOperationException(
                "Inbox administration requires a separate scope outside a transaction."
            );
    }

    private static JsonElement? Snapshot<T>(T? row)
        where T : class
    {
        if (row is null)
            return null;
        var node = JsonSerializer.SerializeToNode(row, _json)!;
        // Expose stored jsonb as JSON values rather than strings containing escaped JSON.
        foreach (var field in new[] { "envelope", "source", "details", "diagnostics", "settings" })
            if (node[field] is JsonValue value && value.TryGetValue<string>(out var json))
                node[field] = JsonNode.Parse(json, documentOptions: new() { MaxDepth = 256 });
        return JsonSerializer.SerializeToElement(node, _json);
    }
}

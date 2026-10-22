using System.Text.Json;
using System.Text.Json.Serialization;

namespace MonixOne.Inbox;

/// <summary>Scoped-адаптер метаданных нестандартного envelope. Полный JSON сохраняется неизменным.</summary>
public interface IInboxEnvelopeAdapter
{
    InboxEventMetadata Extract(JsonElement message);
}

/// <summary>
/// Идентичность факта и исходная sequence, извлечённые из полного сообщения.
/// </summary>
/// <param name="EventId">
/// Уникальный id факта внутри handler, до 512 UTF-8 байт.
/// </param>
/// <param name="Producer">
/// Источник счётчика, до 256 UTF-8 байт.
/// </param>
/// <param name="SequenceScope">
/// Область исходной нумерации, до 256 UTF-8 байт.
/// </param>
/// <param name="ObjectKey">
/// Ключ объекта с необходимой tenant/type областью, до 1024 UTF-8 байт.
/// </param>
/// <param name="Sequence">
/// Положительный Int64 номер источника; начинается с любого положительного номера.
/// </param>
/// <param name="EventType">
/// Непустое имя типа события.
/// </param>
/// <param name="OccurredAt">
/// Время факта; при сохранении нормализуется в UTC и до микросекунд.
/// </param>
public sealed record InboxEventMetadata(
    [property: JsonPropertyName("eventId")] string EventId,
    [property: JsonPropertyName("producer")] string Producer,
    [property: JsonPropertyName("sequenceScope")] string SequenceScope,
    [property: JsonPropertyName("objectKey")] string ObjectKey,
    [property: JsonPropertyName("sequence")] long Sequence,
    [property: JsonPropertyName("eventType")] string EventType,
    [property: JsonPropertyName("occurredAt")] DateTimeOffset OccurredAt
);

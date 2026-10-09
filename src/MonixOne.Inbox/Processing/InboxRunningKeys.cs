using System.Collections.Concurrent;

namespace MonixOne.Inbox.Processing;

internal sealed record InboxProcessingKey(string HandlerId, string Producer, string Scope, string ObjectKey);

// Only a process-local exclusion set. Replicas are coordinated by the final database CAS.
internal sealed class InboxRunningKeys
{
    private readonly ConcurrentDictionary<InboxProcessingKey, byte> _keys = new();
    internal bool TryAdd(InboxProcessingKey key) => _keys.TryAdd(key, 0);
    internal void Remove(InboxProcessingKey key) => _keys.TryRemove(key, out _);
    internal InboxProcessingKey[] Snapshot(string handler) =>
        _keys.Keys.Where(x => x.HandlerId == handler).ToArray();
}

namespace MonixOne.Inbox.Registration;

// Every intake/dispatcher must await this task before touching PostgreSQL or NATS.
// Hosted-service registration order alone cannot protect concurrent service startup.
internal sealed class InboxStartupBarrier
{
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal Task WaitAsync(CancellationToken cancellationToken) => _ready.Task.WaitAsync(cancellationToken);

    internal void Complete() => _ready.TrySetResult();

    internal void Fail(Exception error)
    {
        _ready.TrySetException(error);
        // Observe the error even when no worker was started; waiters still receive the original exception.
        _ = _ready.Task.Exception;
    }
}

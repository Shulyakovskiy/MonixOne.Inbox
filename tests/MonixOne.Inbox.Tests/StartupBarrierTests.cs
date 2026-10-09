using MonixOne.Inbox.Registration;

namespace MonixOne.Inbox.Tests;

public sealed class StartupBarrierTests
{
    [Fact]
    public async Task A_worker_started_before_initialization_waits_for_readiness()
    {
        var barrier = new InboxStartupBarrier();
        var first = barrier.WaitAsync(TestContext.Current.CancellationToken);
        var second = barrier.WaitAsync(TestContext.Current.CancellationToken);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        barrier.Complete();
        await Task.WhenAll(first, second);
    }

    [Fact]
    public async Task Initialization_failure_reaches_all_waiters()
    {
        var barrier = new InboxStartupBarrier();
        var waiting = barrier.WaitAsync(TestContext.Current.CancellationToken);
        var error = new InvalidOperationException("Schema initialization failed.");
        barrier.Fail(error);

        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => waiting));
        Assert.Same(
            error,
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                barrier.WaitAsync(TestContext.Current.CancellationToken)
            )
        );
    }

    [Fact]
    public async Task Canceling_one_waiter_does_not_cancel_common_readiness()
    {
        var barrier = new InboxStartupBarrier();
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var waiting = barrier.WaitAsync(stopping.Token);
        await stopping.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

        barrier.Complete();
        await barrier.WaitAsync(TestContext.Current.CancellationToken);
    }
}

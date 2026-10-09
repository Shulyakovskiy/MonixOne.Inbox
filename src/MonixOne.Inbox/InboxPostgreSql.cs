using LinqToDB;
using LinqToDB.Data;

namespace MonixOne.Inbox;

// PostgreSQL primitives used inside typed linq2db queries. Arguments remain SQL parameters.
internal static class InboxPostgreSql
{
    internal static Task<string> SetLockTimeoutAsync(DataConnection data, CancellationToken token) =>
        data.SelectAsync(() => SetConfig("lock_timeout", "100ms", true), token);

    [Sql.Function("set_config", ServerSideOnly = true, IsPure = false)]
    private static string SetConfig(string name, string value, bool local) => throw new ServerSideOnlyException(nameof(SetConfig));

    [Sql.Expression("CURRENT_TIMESTAMP", ServerSideOnly = true)]
    internal static DateTimeOffset TransactionTimestamp() => throw new ServerSideOnlyException(nameof(TransactionTimestamp));

    [Sql.Function("clock_timestamp", ServerSideOnly = true, IsPure = false)]
    internal static DateTimeOffset ClockTimestamp() => throw new ServerSideOnlyException(nameof(ClockTimestamp));

    [Sql.Expression("({0} - {1})", ServerSideOnly = true)]
    internal static DateTimeOffset Subtract(DateTimeOffset timestamp, TimeSpan interval) => throw new ServerSideOnlyException(nameof(Subtract));

    [Sql.Expression("CAST(CAST({0} AS jsonb) AS text)", ServerSideOnly = true)]
    internal static string ValidateJson(string json) => throw new ServerSideOnlyException(nameof(ValidateJson));

    [Sql.Expression("({0} = CAST({1} AS jsonb))", ServerSideOnly = true, IsPredicate = true)]
    internal static bool JsonEquals(string stored, string json) => throw new ServerSideOnlyException(nameof(JsonEquals));

    [Sql.Function("current_database", ServerSideOnly = true)]
    internal static string CurrentDatabase() => throw new ServerSideOnlyException(nameof(CurrentDatabase));

    // pg_advisory_xact_lock returns void; projecting IS NULL makes its execution a typed scalar query.
    [Sql.Expression("pg_advisory_xact_lock({0}) IS NULL", ServerSideOnly = true, IsPure = false)]
    internal static bool AdvisoryTransactionLock(long key) => throw new ServerSideOnlyException(nameof(AdvisoryTransactionLock));

    [Sql.Expression("to_regclass({0}) IS NOT NULL", ServerSideOnly = true, IsPredicate = true)]
    internal static bool RelationExists(string name) => throw new ServerSideOnlyException(nameof(RelationExists));
}

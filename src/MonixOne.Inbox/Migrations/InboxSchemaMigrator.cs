using System.Buffers.Binary;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace MonixOne.Inbox.Migrations;

internal static class InboxSchemaMigrator
{
    internal static async Task EnsureReadyAsync(
        DbContext db,
        string schema,
        bool autoMigrate,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        // Only this idempotent schema transaction may be retried by EF's configured execution strategy.
        // No business handler or NATS action runs inside it; a lost COMMIT response is resolved by history.
        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(
            async token =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(
                    IsolationLevel.ReadCommitted,
                    token
                );
                await using var data = db.CreateLinqToDBConnection(transaction);
                if (db.Database.GetCommandTimeout() is { } timeout)
                    data.CommandTimeout = timeout;

                // Schema is already validated as an ASCII identifier. All other values are parameters.
                var quotedSchema = $"\"{schema}\"";
                // Use the server's database name: different PgBouncer aliases may point to the same database.
                var database = await data.SelectAsync(() => InboxPostgreSql.CurrentDatabase(), token);
                var lockKey = LockKey(database, schema);
                logger.LogDebug("Waiting for OrderedInbox schema lock in {Schema}.", schema);
                await data.SelectAsync(() => InboxPostgreSql.AdvisoryTransactionLock(lockKey), token);

                var historyName = $"{quotedSchema}.schema_migrations";
                var historyExists = await data.SelectAsync(() => InboxPostgreSql.RelationExists(historyName), token);
                if (!historyExists)
                {
                    if (!autoMigrate)
                        throw new InvalidOperationException(
                            $"OrderedInbox schema '{schema}' is not installed. Enable AutoMigrate or install "
                                + $"the package schema before startup."
                        );

                    await data.ExecuteAsync($"CREATE SCHEMA IF NOT EXISTS {quotedSchema}", token);
                    await data.ExecuteAsync(
                        $$"""
                        CREATE TABLE {{quotedSchema}}.schema_migrations (
                            version integer NOT NULL,
                            name text NOT NULL,
                            checksum varchar(64) NOT NULL,
                            applied_at timestamptz NOT NULL,
                            CONSTRAINT schema_migrations_pk PRIMARY KEY (version),
                            CONSTRAINT schema_migrations_version_ck
                                CHECK (version > 0),
                            CONSTRAINT schema_migrations_checksum_ck CHECK (checksum ~ '^[0-9a-f]{64}$'),
                            CONSTRAINT schema_migrations_name_ck CHECK (length(name) > 0)
                        )
                        """,
                        token
                    );
                }

                var history = await data.GetTable<AppliedInboxMigration>()
                    .SchemaName(schema)
                    .OrderBy(m => m.Version)
                    .ToListAsyncLinqToDB(token);
                ValidateHistory(history, schema);
                var pending = InboxMigration.All.Where(m => history.All(h => h.Version != m.Version)).ToArray();
                foreach (var migration in pending)
                {
                    if (!autoMigrate)
                        throw new InvalidOperationException(
                            $"OrderedInbox schema '{schema}' requires migration "
                                + $"{migration.Version}/{migration.Name}; AutoMigrate is disabled."
                        );

                    logger.LogInformation(
                        "Applying OrderedInbox migration {MigrationVersion}/{MigrationName} in {Schema}.",
                        migration.Version,
                        migration.Name,
                        schema
                    );
                    await data.ExecuteAsync(
                        migration.Sql.Replace("{{schema}}", quotedSchema, StringComparison.Ordinal),
                        token
                    );
                }
                foreach (var migration in pending)
                {
                    await data.InsertAsync(
                        new AppliedInboxMigration
                        {
                            Version = migration.Version,
                            Name = migration.Name,
                            Checksum = migration.Checksum,
                            AppliedAt = DateTimeOffset.UtcNow,
                        },
                        schemaName: schema,
                        token: token
                    );
                }

                await InboxSchemaContract.ValidateAsync(data, schema, token);
                // DDL, history and advisory lock share the application's connection and this one transaction.
                await transaction.CommitAsync(token);
                logger.LogInformation(
                    "OrderedInbox schema {Schema} is ready; migration version {MigrationVersion}.",
                    schema,
                    InboxMigration.All[^1].Version
                );
            },
            cancellationToken
        );
    }

    internal static long LockKey(string database, string schema)
    {
        // GetHashCode is process-randomized. A fixed hash and byte order give every replica the same key.
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"MonixOne.Inbox\0{database}\0{schema}"));
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    private static void ValidateHistory(IReadOnlyList<AppliedInboxMigration> history, string schema)
    {
        for (var i = 0; i < history.Count; i++)
        {
            var applied = history[i];
            if (
                applied.Version != i + 1
                || string.IsNullOrWhiteSpace(applied.Name)
                || applied.Checksum.Length != 64
                || applied.Checksum.Any(c => !char.IsAsciiHexDigitLower(c))
            )
                throw new InvalidOperationException(
                    $"OrderedInbox schema '{schema}' has invalid or noncontiguous migration history "
                        + $"at version {applied.Version}."
                );

            if (applied.Version > InboxMigration.All[^1].Version)
                throw new InvalidOperationException($"OrderedInbox schema '{schema}' has a newer migration version.");

            var known = InboxMigration.All.FirstOrDefault(m => m.Version == applied.Version);
            if (
                known is not null
                && (
                    applied.Name != known.Name
                    || applied.Checksum != known.Checksum
                )
            )
                throw new InvalidOperationException(
                    $"OrderedInbox migration {applied.Version} in schema '{schema}' differs from the "
                        + $"embedded migration. Name and checksum must remain unchanged."
                );
        }
    }
}

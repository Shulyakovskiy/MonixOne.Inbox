using System.Text.RegularExpressions;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.EntityFrameworkCore;

namespace MonixOne.Inbox.Migrations;

internal static class InboxSchemaContract
{
    // Only coordination/deduplication invariants. Compare definitions, not names alone.
    private static readonly Dictionary<string, string> _constraints = new()
    {
        ["consumer_state.consumer_state_pk"] = "PRIMARY KEY (handler_id, producer, sequence_scope, object_key)",
        ["consumer_state.consumer_state_sequence_ck"] = "CHECK ((last_sequence IS NULL) OR (last_sequence > 0))",
        ["consumer_state.consumer_state_version_ck"] = "CHECK (version >= 0)",
        ["inbox.inbox_pk"] = "PRIMARY KEY (id)",
        ["inbox.inbox_event_uk"] = "UNIQUE (handler_id, event_id)",
        ["inbox.inbox_sequence_uk"] = "UNIQUE (handler_id, producer, sequence_scope, object_key, sequence)",
        ["inbox.inbox_state_fk"] =
            "FOREIGN KEY (handler_id, producer, sequence_scope, object_key) " +
            "REFERENCES {schema}.consumer_state(handler_id, producer, sequence_scope, object_key) ON DELETE RESTRICT",
        ["inbox.inbox_sequence_ck"] = "CHECK (sequence > 0)",
        ["inbox_attempts.inbox_attempts_pk"] = "PRIMARY KEY (attempt_id)",
        ["inbox_attempts.inbox_attempts_number_uk"] = "UNIQUE (inbox_id, run_id, attempt_number)",
        ["inbox_attempts.inbox_attempts_inbox_fk"] =
            "FOREIGN KEY (inbox_id) REFERENCES {schema}.inbox(id) ON DELETE RESTRICT",
        ["inbox_dlq.inbox_dlq_pk"] = "PRIMARY KEY (id)",
        ["inbox_dlq.inbox_dlq_delivery_uk"] = "UNIQUE (handler_id, deduplication_key)",
        ["inbox_dlq.inbox_dlq_inbox_fk"] =
            "FOREIGN KEY (inbox_id) REFERENCES {schema}.inbox(id) ON DELETE RESTRICT",
        ["inbox_dlq.inbox_dlq_related_fk"] =
            "FOREIGN KEY (related_inbox_id) REFERENCES {schema}.inbox(id) ON DELETE RESTRICT",
        ["inbox_journal.inbox_journal_decision_uk"] = "UNIQUE (decision_id)",
        ["inbox_journal.inbox_journal_inbox_fk"] =
            "FOREIGN KEY (inbox_id) REFERENCES {schema}.inbox(id) ON DELETE RESTRICT",
        ["inbox_journal.inbox_journal_dlq_fk"] =
            "FOREIGN KEY (dlq_id) REFERENCES {schema}.inbox_dlq(id) ON DELETE RESTRICT",
        ["inbox_journal.inbox_journal_attempt_fk"] =
            "FOREIGN KEY (attempt_id) REFERENCES {schema}.inbox_attempts(attempt_id) ON DELETE RESTRICT",
    };

    internal static async Task ValidateAsync(DataConnection data, string schema, CancellationToken token)
    {
        var namespaces = data.GetTable<InboxPostgreSqlCatalog.Namespace>();
        var relations = data.GetTable<InboxPostgreSqlCatalog.Relation>();
        var attributes = data.GetTable<InboxPostgreSqlCatalog.Attribute>();
        var types = data.GetTable<InboxPostgreSqlCatalog.Type>();
        var columns = await (from n in namespaces
            join c in relations on n.Oid equals c.NamespaceOid
            join a in attributes on c.Oid equals a.RelationOid
            join t in types on a.TypeOid equals t.Oid
            where n.Name == schema && a.Number > 0 && !a.Dropped
            select new { Table = c.Name, a.Name, Type = t.Name, a.NotNull }).ToListAsyncLinqToDB(token);
        foreach (var required in new[]
        {
            ("consumer_state", "last_sequence", "int8", false), ("consumer_state", "version", "int8", true),
            ("inbox", "raw_payload", "bytea", true), ("inbox", "attempt_count", "int4", true),
            ("inbox", "received_at", "timestamptz", true), ("inbox", "run_id", "uuid", true),
        })
            if (!columns.Any(c => c.Table == required.Item1 && c.Name == required.Item2 &&
                c.Type == required.Item3 && c.NotNull == required.Item4))
                throw new InvalidOperationException(
                    $"OrderedInbox schema '{schema}': changed column '{required.Item1}.{required.Item2}'.");

        var constraints = await (from c in data.GetTable<InboxPostgreSqlCatalog.Constraint>()
            join t in relations on c.RelationOid equals t.Oid
            join n in namespaces on t.NamespaceOid equals n.Oid
            where n.Name == schema && c.Validated && !c.Deferrable
            select new { Key = t.Name + "." + c.Name,
                Definition = InboxPostgreSqlCatalog.ConstraintDefinition(c.Oid) }).ToListAsyncLinqToDB(token);
        foreach (var (key, definition) in _constraints)
            if (!constraints.Any(c => c.Key == key && Normalize(c.Definition) ==
                Normalize(definition.Replace("{schema}", schema, StringComparison.Ordinal))))
                throw new InvalidOperationException($"OrderedInbox schema '{schema}': changed constraint '{key}'.");

        var indexes = await (from i in data.GetTable<InboxPostgreSqlCatalog.Index>()
            join c in relations on i.IndexOid equals c.Oid
            join am in data.GetTable<InboxPostgreSqlCatalog.AccessMethod>() on c.AccessMethodOid equals am.Oid
            join n in namespaces on c.NamespaceOid equals n.Oid
            where n.Name == schema && c.Name == "inbox_active_head_idx" && am.Name == "btree" &&
                i.Valid && i.Ready && i.Predicate != null
            select new { i.RelationOid, i.Keys, i.KeyCount,
                Predicate = InboxPostgreSqlCatalog.IndexPredicate(i.Predicate!, i.RelationOid) })
            .ToListAsyncLinqToDB(token);
        var predicates = new List<string>();
        foreach (var index in indexes)
        {
            var names = await attributes.Where(a => a.RelationOid == index.RelationOid)
                .Select(a => new { a.Number, a.Name }).ToListAsyncLinqToDB(token);
            // indkey includes INCLUDE columns; only the first indnkeyatts belong to the index key.
            var keys = index.Keys.Take(index.KeyCount).Join(names, k => k, a => a.Number, (_, a) => a.Name);
            if (keys.SequenceEqual(["handler_id", "producer", "sequence_scope", "object_key", "sequence"]))
                predicates.Add(index.Predicate);
        }
        if (predicates.Count != 1 || NormalizePredicate(predicates[0]) != "status=anyarray['pending','retry']")
            throw new InvalidOperationException($"OrderedInbox schema '{schema}': invalid inbox_active_head_idx.");
    }

    private static string NormalizePredicate(string predicate) =>
        Normalize(Regex.Replace(predicate, @"::(?:text|character varying)(?:\[\])?", ""));

    private static string Normalize(string definition) =>
        Regex.Replace(definition, "[\\s()\\\"]", "").ToLowerInvariant();
}

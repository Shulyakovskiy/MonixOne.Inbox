using LinqToDB;
using LinqToDB.Mapping;

namespace MonixOne.Inbox.Migrations;

// Read-only mappings of the PostgreSQL catalog; never include these in application migrations.
internal static class InboxPostgreSqlCatalog
{
    [Sql.Function("pg_get_constraintdef", ServerSideOnly = true)]
    internal static string ConstraintDefinition(uint oid) => throw new ServerSideOnlyException(nameof(ConstraintDefinition));

    [Sql.Function("pg_get_expr", ServerSideOnly = true)]
    internal static string IndexPredicate(string expression, uint relation) => throw new ServerSideOnlyException(nameof(IndexPredicate));

    [Table("pg_namespace", Schema = "pg_catalog")]
    internal sealed class Namespace
    {
        [Column("oid")] public uint Oid { get; set; }
        [Column("nspname"), NotNull] public string Name { get; set; } = "";
    }

    [Table("pg_class", Schema = "pg_catalog")]
    internal sealed class Relation
    {
        [Column("oid")] public uint Oid { get; set; }
        [Column("relnamespace")] public uint NamespaceOid { get; set; }
        [Column("relam")] public uint AccessMethodOid { get; set; }
        [Column("relname"), NotNull] public string Name { get; set; } = "";
    }

    [Table("pg_attribute", Schema = "pg_catalog")]
    internal sealed class Attribute
    {
        [Column("attrelid")] public uint RelationOid { get; set; }
        [Column("atttypid")] public uint TypeOid { get; set; }
        [Column("attnum")] public short Number { get; set; }
        [Column("attname"), NotNull] public string Name { get; set; } = "";
        [Column("attnotnull")] public bool NotNull { get; set; }
        [Column("attisdropped")] public bool Dropped { get; set; }
    }

    [Table("pg_type", Schema = "pg_catalog")]
    internal sealed class Type
    {
        [Column("oid")] public uint Oid { get; set; }
        [Column("typname"), NotNull] public string Name { get; set; } = "";
    }

    [Table("pg_constraint", Schema = "pg_catalog")]
    internal sealed class Constraint
    {
        [Column("oid")] public uint Oid { get; set; }
        [Column("conrelid")] public uint RelationOid { get; set; }
        [Column("conname"), NotNull] public string Name { get; set; } = "";
        [Column("convalidated")] public bool Validated { get; set; }
        [Column("condeferrable")] public bool Deferrable { get; set; }
    }

    [Table("pg_index", Schema = "pg_catalog")]
    internal sealed class Index
    {
        [Column("indexrelid")] public uint IndexOid { get; set; }
        [Column("indrelid")] public uint RelationOid { get; set; }
        [Column("indisvalid")] public bool Valid { get; set; }
        [Column("indisready")] public bool Ready { get; set; }
        [Column("indnkeyatts")] public short KeyCount { get; set; }
        [Column("indkey", DbType = "int2vector"), NotNull] public short[] Keys { get; set; } = [];
        [Column("indpred", DbType = "pg_node_tree")] public string? Predicate { get; set; }
    }

    [Table("pg_am", Schema = "pg_catalog")]
    internal sealed class AccessMethod
    {
        [Column("oid")] public uint Oid { get; set; }
        [Column("amname"), NotNull] public string Name { get; set; } = "";
    }
}

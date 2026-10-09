using System.Security.Cryptography;
using System.Text;
using LinqToDB.Mapping;

namespace MonixOne.Inbox.Migrations;

internal sealed record InboxMigration(int Version, string Name, string Sql, string Checksum)
{
    internal static IReadOnlyList<InboxMigration> All { get; } =
        [Load(1, "initial"), Load(2, "state_metrics"), Load(3, "optimistic_processing")];
    internal static InboxMigration Initial => All[0];

    private static InboxMigration Load(int version, string name)
    {
        var assembly = typeof(InboxMigration).Assembly;
        using var stream =
            assembly.GetManifestResourceStream($"MonixOne.Inbox.Migrations.{version:000}_{name}.sql")
            ?? throw new InvalidOperationException($"Missing embedded OrderedInbox migration {version}/{name}.");
        using var reader = new StreamReader(stream, Encoding.UTF8);
        // Git/editor line-ending conversion must not change an already installed migration's checksum.
        var sql = reader.ReadToEnd().Replace("\r\n", "\n", StringComparison.Ordinal);
        var checksum = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));
        return new(version, name, sql, checksum);
    }
}

[Table("schema_migrations")]
internal sealed class AppliedInboxMigration
{
    [PrimaryKey, Column("version")]
    public int Version { get; set; }

    [Column("name"), NotNull]
    public string Name { get; set; } = "";

    [Column("checksum"), NotNull]
    public string Checksum { get; set; } = "";

    [Column("applied_at")]
    public DateTimeOffset AppliedAt { get; set; }
}

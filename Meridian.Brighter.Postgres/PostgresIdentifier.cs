using System.Text.RegularExpressions;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Table and schema names come from configuration and are written into SQL, so only plain
/// identifiers are accepted.
/// </summary>
internal static partial class PostgresIdentifier
{
    public static string Validate(string identifier) =>
        Plain().IsMatch(identifier)
            ? identifier
            : throw new InvalidOperationException(
                $"'{identifier}' is not a plain PostgreSQL identifier (letters, digits and underscores)."
            );

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Plain();
}

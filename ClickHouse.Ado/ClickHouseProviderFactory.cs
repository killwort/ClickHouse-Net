using System.Data.Common;

namespace ClickHouse.Ado;

/// <summary>
///     Clickhouse specific database provider factory.
/// </summary>
public class ClickHouseProviderFactory : DbProviderFactory {
#if CORE_FRAMEWORK
#if MODERN_CORE_FRAMEWORK
    /// <inheritdoc />
    public override bool CanCreateBatch => false;
#endif
    /// <inheritdoc />
    public override bool CanCreateCommandBuilder => false;
    /// <inheritdoc />
    public override bool CanCreateDataAdapter => false;
#endif
    /// <inheritdoc />
    public override bool CanCreateDataSourceEnumerator => false;

    /// <inheritdoc />
    public override DbCommand CreateCommand() => new ClickHouseCommand();

    /// <inheritdoc />
    public override DbConnection CreateConnection() => new ClickHouseConnection();

    /// <inheritdoc />
    public override DbParameter CreateParameter() => new ClickHouseParameter();
}
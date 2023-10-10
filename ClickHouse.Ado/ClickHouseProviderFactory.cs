using System.Data.Common;

namespace ClickHouse.Ado;

public class ClickHouseProviderFactory : DbProviderFactory {
    public override bool CanCreateBatch => false;
    public override bool CanCreateCommandBuilder => false;
    public override bool CanCreateDataAdapter => false;
    public override bool CanCreateDataSourceEnumerator => false;
    public override DbCommand CreateCommand() => new ClickHouseCommand();
    public override DbConnection CreateConnection() => new ClickHouseConnection();
    public override DbParameter CreateParameter() => new ClickHouseParameter();
}

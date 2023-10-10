using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ColumnTypes;

namespace ClickHouse.Ado.Impl.Data;

internal class ColumnInfo {
    public string Name { get; set; }
    public ColumnType Type { get; set; }

    internal async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        await formatter.WriteString(Name, cToken);
        await formatter.WriteString(Type.AsClickHouseType(ClickHouseTypeUsageIntent.ColumnInfo), cToken);

        if (rows > 0)
            await Type.Write(formatter, rows, cToken);
    }

    public static async Task<ColumnInfo> Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        var rv = new ColumnInfo();
        rv.Name = await formatter.ReadString(cToken);
        rv.Type = ColumnType.Create(await formatter.ReadString(cToken));
        if (rows > 0)
            await rv.Type.Read(formatter, rows, cToken);
        return rv;
    }
}
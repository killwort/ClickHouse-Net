using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data; 

internal class Block {
    public string Name { get; set; } = "";
    public BlockInfo BlockInfo { get; set; } = new();

    public int Rows => Columns.Count > 0 ? Columns.First().Type.Rows : 0;

    public List<ColumnInfo> Columns { get; } = new();

    internal async Task Write(ProtocolFormatter formatter, CancellationToken cToken) {
        await formatter.WriteUInt((int)ClientMessageType.Data, cToken);
        if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
            await formatter.WriteString(Name, cToken);
        using (formatter.Compression) {
            if (formatter.ClientInfo.ClientRevision >= ProtocolCaps.DbmsMinRevisionWithBlockInfo) await BlockInfo.Write(formatter, cToken);

            await formatter.WriteUInt(Columns.Count, cToken);
            await formatter.WriteUInt(Rows, cToken);

            foreach (var column in Columns) await column.Write(formatter, Rows, cToken);
        }
    }

    public static async Task<Block> Read(ProtocolFormatter formatter, CancellationToken cToken) {
        var rv = new Block();
        if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
            await formatter.ReadString(cToken);
        using (formatter.Decompression) {
            if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithBlockInfo)
                rv.BlockInfo = await BlockInfo.Read(formatter, cToken);

            var cols = await formatter.ReadUInt(cToken);
            var rows = await formatter.ReadUInt(cToken);
            for (var i = 0; i < cols; i++) rv.Columns.Add(await ColumnInfo.Read(formatter, (int)rows, cToken));
        }

        return rv;
    }
}
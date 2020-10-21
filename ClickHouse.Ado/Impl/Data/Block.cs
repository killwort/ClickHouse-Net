using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Ado.Impl.Data {
    internal class Block {
        public string Name { get; set; } = "";
        public BlockInfo BlockInfo { get; set; } = new BlockInfo();

        public int Rows => Columns.Count > 0 ? Columns.First().Type.Rows : 0;

        public List<ColumnInfo> Columns { get; } = new List<ColumnInfo>();

        internal void Write(ProtocolFormatter formatter) {
            formatter.WriteUInt((int) ClientMessageType.Data);
            if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
                formatter.WriteString(Name);
            using (formatter.Compression) {
                if (formatter.ClientInfo.ClientRevision >= ProtocolCaps.DbmsMinRevisionWithBlockInfo) BlockInfo.Write(formatter);

                formatter.WriteUInt(Columns.Count);
                formatter.WriteUInt(Rows);

                foreach (var column in Columns) column.Write(formatter, Rows);
            }
        }

        public static Block Read(ProtocolFormatter formatter) {
            var rv = new Block();
            if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
                formatter.ReadString();
            using (formatter.Decompression) {
                if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithBlockInfo)
                    rv.BlockInfo = BlockInfo.Read(formatter);

                var cols = formatter.ReadUInt();
                var rows = formatter.ReadUInt();
                for (var i = 0; i < cols; i++) rv.Columns.Add(ColumnInfo.Read(formatter, (int) rows));
            }

            return rv;
        }
    }
}
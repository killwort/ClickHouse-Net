using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class LowCardinalityColumnType : ColumnType {
        public LowCardinalityColumnType(ColumnType innerType) => InnerType = innerType;

        public override int Rows => Indices?.Length ?? 0;
        internal override Type CLRType => InnerType.CLRType;

        public ColumnType InnerType { get; }

        private int _keySize;

        private int[] Indices;

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"LowCardinality({InnerType.AsClickHouseType(usageIntent)})";

        public override void Write(ProtocolFormatter formatter, int rows) {
            // This is rather naive implementation of writing - without any deduplication, however who cares?
            // Clickhouse server will re-deduplicate inserted values anyway.
            formatter.WriteBytes(BitConverter.GetBytes(1L));
            formatter.WriteBytes(BitConverter.GetBytes(1538L));
            formatter.WriteBytes(BitConverter.GetBytes((long) rows));
            InnerType.Write(formatter, rows);
            formatter.WriteBytes(BitConverter.GetBytes((long) rows));
            for (var i = 0; i < rows; i++)
                formatter.WriteBytes(BitConverter.GetBytes(i));
        }

        internal override void Read(ProtocolFormatter formatter, int rows) {
            var version = BitConverter.ToInt64(formatter.ReadBytes(8), 0);
            if (version != 1)
                throw new NotSupportedException("Invalid LowCardinality dictionary version");
            var keyLength = BitConverter.ToInt64(formatter.ReadBytes(8), 0);
            _keySize = 1 << (byte) (keyLength & 0xff);
            if (_keySize < 0 || _keySize > 4) //LowCardinality with >4e9 keys? WTF???
                throw new NotSupportedException("Invalid LowCardinality key size");
            if (((keyLength >> 8) & 0xff) != 6)
                throw new NotSupportedException("Invalid LowCardinality key flags");
            var keyCount = BitConverter.ToInt64(formatter.ReadBytes(8), 0);
            InnerType.Read(formatter, (int) keyCount);
            var valueCount = BitConverter.ToInt64(formatter.ReadBytes(8), 0);
            Indices = new int[rows];
            for (var i = 0; i < rows; i++) {
                Indices[i] = BitConverter.ToInt32(formatter.ReadBytes(_keySize, 4), 0);
            }
        }

        public override void ValueFromConst(Parser.ValueType val) {
            InnerType.ValueFromConst(val);
            Indices = new int[InnerType.Rows];
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            InnerType.ValueFromParam(parameter);
            Indices = new int[InnerType.Rows];
        }

        public override object Value(int currentRow) => InnerType.Value(Indices[currentRow]);

        public override long IntValue(int currentRow) { return InnerType.IntValue(Indices[currentRow]); }

        public override void ValuesFromConst(IEnumerable objects) {
            InnerType.NullableValuesFromConst(objects);
            Indices = new int[InnerType.Rows];
        }
    }
}

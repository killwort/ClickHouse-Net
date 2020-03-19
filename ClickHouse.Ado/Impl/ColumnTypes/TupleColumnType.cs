using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class TupleColumnType : ColumnType {
        public TupleColumnType(IEnumerable<ColumnType> values) => Columns = values.ToArray();

        public ColumnType[] Columns { get; }

        public override int Rows => Columns.First().Rows;
        internal override Type CLRType => typeof(Tuple<>).MakeGenericType(Columns.Select(x => x.CLRType).ToArray());

        internal override void Read(ProtocolFormatter formatter, int rows) {
            foreach (var column in Columns)
                column.Read(formatter, rows);
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Tuple({string.Join(",", Columns.Select(x => x.AsClickHouseType(usageIntent)))})";

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var column in Columns) column.Write(formatter, rows);
        }

        public override void ValueFromConst(Parser.ValueType val) => throw new NotSupportedException();

        public override void ValueFromParam(ClickHouseParameter parameter) => throw new NotSupportedException();

        public override object Value(int currentRow) => Columns.Select(x => x.Value(currentRow)).ToArray();

        public override long IntValue(int currentRow) => throw new InvalidCastException();

        public override void ValuesFromConst(IEnumerable objects) => throw new NotSupportedException();
    }
}
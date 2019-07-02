using System;
using System.Collections;
using System.Diagnostics;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class NullColumnType:ColumnType {
        private int _rows;

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            new SimpleColumnType<byte>().Read(formatter, rows);
            _rows = rows;
        }

        public override int Rows => _rows;
        internal override Type CLRType => typeof(object);

        public override void ValueFromConst(Parser.ValueType val)
        {
            
        }

        public override string AsClickHouseType()
        {
            return "Null";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            new SimpleColumnType<byte>(new byte[rows]).Read(formatter, rows);
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            
        }

        public override object Value(int currentRow)
        {
            return null;
        }

        public override long IntValue(int currentRow)
        {
            return 0;
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            
        }

        public override void NullableValuesFromConst(IEnumerable objects)
        {

        }
    }
}
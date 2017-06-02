using System;
using System.Collections;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class ArrayColumnType : ColumnType
    {
        public ArrayColumnType(ColumnType innerType)
        {
            InnerType = innerType;
        }

        public ColumnType InnerType { get; private set; }
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            throw new NotImplementedException();
            var offsets=new SimpleColumnType<ulong>();
            offsets.Read(formatter, rows);
            InnerType.Read(formatter, rows);
        }

        public override int Rows => InnerType.Rows;
        internal override Type CLRType => InnerType.CLRType.MakeArrayType();

        public override void ValueFromConst(string value, Parser.ConstType typeHint)
        {
            throw new NotImplementedException();
        }

        public override string AsClickHouseType()
        {
            return $"Array({InnerType.AsClickHouseType()})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            throw new System.NotImplementedException();
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            throw new NotImplementedException();
        }

        public override object Value(int currentRow)
        {
            throw new NotImplementedException();
        }

        public override long IntValue(int currentRow)
        {
            throw new InvalidCastException();
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            throw new NotImplementedException();
        }
    }
}
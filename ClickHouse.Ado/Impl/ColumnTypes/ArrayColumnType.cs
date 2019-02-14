using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class ArrayColumnType : ColumnType
    {
        public ArrayColumnType(ColumnType innerType)
        {
            Offsets = new SimpleColumnType<ulong>();
            InnerType = innerType;
        }

        public ColumnType InnerType { get; private set; }
        public SimpleColumnType<ulong> Offsets { get; private set; }
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Offsets.Read(formatter, rows);
            var totalRows = Offsets.Data.Last();
            InnerType.Read(formatter,(int) totalRows);
        }

        public override int Rows => InnerType.Rows;
        internal override Type CLRType => InnerType.CLRType.MakeArrayType();

        public override void ValueFromConst(Parser.ValueType val)
        {
            if (val.TypeHint == Parser.ConstType.Array)
            {
                InnerType.ValuesFromConst(val.ArrayValue.Select(x => Convert.ChangeType(x.StringValue, InnerType.CLRType)));
                Offsets.ValueFromConst(new Parser.ValueType {TypeHint = Parser.ConstType.Number, StringValue = InnerType.Rows.ToString()});
            }
            else
                throw new NotSupportedException();
            
        }

        public override string AsClickHouseType()
        {
            return $"Array({InnerType.AsClickHouseType()})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Offsets.Write(formatter,rows);
            var totalRows=rows==0?0:Offsets.Data.Last();
            InnerType.Write(formatter, (int)totalRows);
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            if (parameter.DbType == 0
#if !NETCOREAPP11
                || parameter.DbType == System.Data.DbType.Object
#endif
            )
                ValuesFromConst(new[] {parameter.Value as IEnumerable});
            else throw new NotSupportedException();
        }

        public override object Value(int currentRow)
        {
            var start = currentRow == 0 ? 0 : Offsets.Data[currentRow - 1];
            var end = Offsets.Data[currentRow];
            var rv = new object[end - start];
            for (var i = start; i < end; i++)
                rv[i-start]=InnerType.Value((int)i);
            return rv;
        }

        public override long IntValue(int currentRow)
        {
            throw new InvalidCastException();
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            var offsets = new List<ulong>();
            var itemsPlain = new List<object>();
            ulong currentOffset = 0;
            foreach (var item in objects)
            {
                ulong itemCount = 0;
                foreach (var itemPart in item)
                {
                    itemCount++;
                    itemsPlain.AddRange(itemPart);
                }
                currentOffset += itemCount;
                offsets.Add(currentOffset);
            }
            
            Offsets.ValuesFromConst(offsets);
            InnerType.ValuesFromConst(itemsPlain);
        }
    }
}
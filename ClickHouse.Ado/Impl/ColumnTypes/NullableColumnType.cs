using System;
using System.Collections;
#if !NETCOREAPP11
using System.Data;
#endif
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class NullableColumnType : ColumnType
    {
        public NullableColumnType(ColumnType innerType)
        {
            InnerType = innerType;
        }

        public override bool IsNullable => true;
        public override int Rows => InnerType.Rows;
        internal override Type CLRType => InnerType.CLRType.IsByRef ? InnerType.CLRType : typeof(Nullable<>).MakeGenericType(InnerType.CLRType);

        public override string AsClickHouseType()
        {
            return $"Nullable({InnerType.AsClickHouseType()})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            new SimpleColumnType<byte>(Nulls.Select(x => x ? (byte)1 : (byte)0).ToArray()).Write(formatter, rows);
            InnerType.Write(formatter, rows);
        }

        public ColumnType InnerType { get; private set; }
        public bool[] Nulls { get; private set; }
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            var nullStatuses = new SimpleColumnType<byte>();
            nullStatuses.Read(formatter, rows);
            Nulls = nullStatuses.Data.Select(x => x != 0).ToArray();
            InnerType.Read(formatter, rows);
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            Nulls = new[] { val.StringValue == null && val.ArrayValue==null };
            InnerType.ValueFromConst(val);
        }
        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            Nulls = new[] { parameter.Value==null };
            InnerType.ValueFromParam(parameter);
        }

        public override object Value(int currentRow)
        {
            return Nulls[currentRow]?null:InnerType.Value(currentRow);
        }

        public override long IntValue(int currentRow)
        {
            if(Nulls[currentRow])
#if NETSTANDARD15 || NETCOREAPP11
				throw new ArgumentNullException();
#else
				throw new System.Data.SqlTypes.SqlNullValueException();
#endif
			return InnerType.IntValue(currentRow);
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            InnerType.NullableValuesFromConst(objects);
            Nulls = objects.Cast<object>().Select(x => x == null).ToArray();
            //Data = objects.Cast<DateTime>().ToArray();
        }

        public bool IsNull(int currentRow)
        {
            return Nulls[currentRow];
        }
    }
}
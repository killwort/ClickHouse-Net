using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class EnumColumnType : ColumnType
    {

        public EnumColumnType(int baseSize, IEnumerable<Tuple<string, int>> values)
        {
            Values = values;
            BaseSize = baseSize;
        }

        public IEnumerable<Tuple<string, int>> Values { get; private set; }
        public int BaseSize { get; private set; }
        public int[] Data { get; private set; }
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            if (BaseSize == 8)
            {
                var vals = new SimpleColumnType<byte>();
                Data = vals.Data.Select(x => (int)x).ToArray();
            }
            else if (BaseSize == 16)
            {
                var vals = new SimpleColumnType<short>();
                Data = vals.Data.Select(x => (int) x).ToArray();
            }
            else
            {
                throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
            }
        }

        public override int Rows => Data.Length;
        
        public override string AsClickHouseType()
        {
            return $"Enum{BaseSize}({string.Join(",", Values.Select(x => $"{ProtocolFormatter.EscapeStringValue(x.Item1)}={x.Item2}"))})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            if (BaseSize == 8)
            {
                new SimpleColumnType<byte>(Data.Select(x => (byte)x).ToArray()).Write(formatter, rows);
            }
            else if (BaseSize == 16)
            {
                new SimpleColumnType<short>(Data.Select(x => (short)x).ToArray()).Write(formatter, rows);
            }
            else
            {
                throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
            }
        }

        public override void ValueFromConst(string value, Parser.ConstType typeHint)
        {
            if (typeHint == Parser.ConstType.String)
            {
                var uvalue = ProtocolFormatter.UnescapeStringValue(value);
                Data = new[] { Values.First(x => x.Item1 == uvalue).Item2 };
            }
            else
                Data = new[] { int.Parse(value) };
        }
        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            if (parameter.DbType == DbType.String || parameter.DbType == DbType.StringFixedLength || parameter.DbType == DbType.AnsiString || parameter.DbType == DbType.AnsiStringFixedLength)
                Data = new[] { Values.First(x => x.Item1 == parameter.Value?.ToString()).Item2 };
            else if (parameter.DbType == DbType.Int16 || parameter.DbType == DbType.Int32 || parameter.DbType == DbType.Int64
                || parameter.DbType == DbType.UInt16 || parameter.DbType == DbType.UInt32 || parameter.DbType == DbType.UInt64)
                Data = new[] { (int)Convert.ChangeType(parameter.Value,typeof(int)) };
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Enum.");
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            return Data[currentRow];
        }
    }
}
using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class StringColumnType : ColumnType
    {
        public StringColumnType()
        {
        }

        public StringColumnType(string[] data)
        {
            Data = data;
        }

        public string[] Data { get; private set; }

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Data=new string[rows];
            for (var i = 0; i < rows; i++)
            {
                Data[i] = formatter.ReadString();
            }
        }

        public override int Rows => Data?.Length ?? 0;
        public override string AsClickHouseType()
        {
            return "String";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
            {
                formatter.WriteString(d);
            }
        }

        public override void ValueFromConst(string value, Parser.ConstType typeHint)
        {
            if (typeHint == Parser.ConstType.String)
            {
                var uvalue = ProtocolFormatter.UnescapeStringValue(value);
                Data = new[] { uvalue };
            }
            else
                Data = new[] { value };
        }
        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            Data = new[] { parameter.Value?.ToString() };
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            throw new InvalidCastException();
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<string>().ToArray();
        }
    }
}
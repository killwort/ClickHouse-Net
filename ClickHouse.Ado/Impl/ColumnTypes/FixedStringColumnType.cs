﻿using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using System.Text;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class FixedStringColumnType : ColumnType
    {
        public FixedStringColumnType(uint length)
        {
            Length = length;
        }

        public uint Length { get; private set; }
        public string[] Data { get; private set; }
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Data = new string[rows];
            var bytes = formatter.ReadBytes((int)(rows * Length));
            for (var i = 0; i < rows; i++)
                Data[i] = Encoding.UTF8.GetString(bytes, (int) (i*Length), (int) Length);
        }

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(string);

        public override string AsClickHouseType()
        {
            return $"FixedString({Length})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
            {
                var bytes = Encoding.UTF8.GetBytes(d);
                var left = Length - bytes.Length;
                if (left <= 0)
                    formatter.WriteBytes(bytes.Take((int) Length).ToArray());
                else
                {
                    formatter.WriteBytes(bytes);
                    formatter.WriteBytes(new byte[left]);
                }
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            if (val.TypeHint == Parser.ConstType.String)
            {
                var uvalue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
                Data = new[] { uvalue };
            }
            else
                Data = new[] { val.StringValue };
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

        public override void NullableValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<string>().Select(s => s ?? string.Empty).ToArray();
        }
    }
}
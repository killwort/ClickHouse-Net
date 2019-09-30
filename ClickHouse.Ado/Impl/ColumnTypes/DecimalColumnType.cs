using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class DecimalColumnType : ColumnType {
        private readonly uint _length;
        private readonly int _byteLength;
        private readonly uint _precision;
        private decimal _exponent;

        public DecimalColumnType(uint length, uint precision) {
            _length = length;
            _precision = precision;
            if (_length >= 28)
                throw new ClickHouseException($"Decimals with length >= 28 are not supported (.NET framework decimal range limit)");
            if (length <= 9)
                _byteLength = 4;
            else if (length <= 18)
                _byteLength = 8;
            else if (length <= 38)
                _byteLength = 16;
            else throw new ClickHouseException($"Invalid Decimal length {length}");
            _exponent = (decimal) Math.Pow(10, precision);
        }

        public decimal[] Data { get; private set; }

        internal override void Read(ProtocolFormatter formatter, int rows) {
            Data = new decimal[rows];
            var bytes = formatter.ReadBytes((int) (rows * _byteLength));
            for (var i = 0; i < rows; i++) {
                if (_byteLength == 4)
                    Data[i] = BitConverter.ToInt32(bytes, i * _byteLength) / _exponent;
                else if (_byteLength == 4)
                    Data[i] = BitConverter.ToInt64(bytes, i * _byteLength) / _exponent;
                else {
                    byte c = (byte) ((bytes[(1 + i) * _byteLength - 1] & 0x80) != 0 ? 0xff : 0);
                    decimal current = 0;
                    for (var k = 0; k < _byteLength; k++) {
                        current *= 0x100;
                        current += c ^ bytes[(1 + i) * _byteLength - k - 1];
                    }

                    Data[i] = (c != 0 ? -(current + 1) : current) / _exponent;
                }
            }
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data) {
                var premultiplied = d * _exponent;
                if(_byteLength==4)
                    formatter.WriteBytes(BitConverter.GetBytes((int)premultiplied));
                else if(_byteLength==8)
                    formatter.WriteBytes(BitConverter.GetBytes((long)premultiplied));
                else {
                    byte c = (byte) (premultiplied > 0 ? 0 : 0xff);
                    if (c != 0) premultiplied = -premultiplied - 1;
                    for (var i = 0; i < _byteLength; i++) {
                        byte next = (byte) ((byte) (Math.Truncate(premultiplied) % 0xff) ^ c);
                        premultiplied = Math.Truncate(premultiplied / 0x100);
                        formatter.WriteByte(next);
                    }
                }
            }
        }

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(decimal);

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] {(decimal) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue), typeof(decimal))};
            else if (val.TypeHint == Parser.ConstType.Number)
                Data = new[] {(decimal) Convert.ChangeType(val.StringValue, typeof(decimal))};
            else
                throw new NotSupportedException();
        }

        public override string AsClickHouseType() => $"Decimal({_length}, {_precision})";

        public override void ValueFromParam(ClickHouseParameter parameter) => Data = new[] {(decimal) Convert.ChangeType(parameter.Value, typeof(decimal))};

        public override object Value(int currentRow) => Data[currentRow];

        public override long IntValue(int currentRow) => (long) Data[currentRow];

        public override void ValuesFromConst(IEnumerable objects)
            => Data = objects.Cast<decimal>().ToArray();

        public override void NullableValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<decimal?>()
                .Select(x => x ?? 0m)
                .ToArray();
        }
    }
}
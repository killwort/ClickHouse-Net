using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class DecimalColumnType : ColumnType {
        private readonly int _byteLength;
        private readonly decimal _exponent;
        private readonly uint _length;
        private readonly uint _precision;

        public DecimalColumnType(uint length, uint precision) {
            _length = length;
            _precision = precision;
            if (_length >= 28)
                throw new ClickHouseException("Decimals with length >= 28 are not supported (.NET framework decimal range limit)");
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

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(decimal);

        internal override void Read(ProtocolFormatter formatter, int rows) {
            Data = new decimal[rows];
            var bytes = formatter.ReadBytes(rows * _byteLength);
            for (var i = 0; i < rows; i++)
                if (_byteLength == 4) {
                    Data[i] = BitConverter.ToInt32(bytes, i * _byteLength) / _exponent;
                } else if (_byteLength == 8) {
                    Data[i] = BitConverter.ToInt64(bytes, i * _byteLength) / _exponent;
                } else {
                    var premultiplied = new BigInteger(bytes.Skip(i * _byteLength).Take(_byteLength).ToArray());
                    var result = (decimal) BigInteger.DivRem(premultiplied, new BigInteger(_exponent), out var remainder);
                    result += (decimal) remainder / _exponent;
                    Data[i] = result;
                }
        }
        private static readonly int[] _lookup =
        {
            32, 0, 1, 26, 2, 23, 27, 0, 3, 16, 24, 30, 28, 11, 0, 13, 4, 7, 17,
            0, 25, 22, 31, 15, 29, 10, 12, 6, 0, 21, 14, 9, 5, 20, 8, 19, 18
        };
        private static int NTZ(int n) {
            return _lookup[(n & -n) % 37];
        }
        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data) {
                var premultiplied = new BigInteger(d * _exponent);
                if (_byteLength == 4) {
                    formatter.WriteBytes(BitConverter.GetBytes((int) premultiplied));
                } else if (_byteLength == 8) {
                    formatter.WriteBytes(BitConverter.GetBytes((long) premultiplied));
                } else {
                    var neg = premultiplied < 0;
                    if (neg) premultiplied = -premultiplied;
                    var bytes = premultiplied.ToByteArray();
                    if (neg) {
                        var ntzPassed = false;
                        for (var i = 0; i < _byteLength; i++) {
                            var cb = i < bytes.Length ? bytes[i] : (byte) 0;
                            if (ntzPassed)
                                cb = (byte) ~cb;
                            else {
                                var ntz = NTZ(cb);
                                if ( ntz< 8) {
                                    ntzPassed = true;
                                    cb = (byte) ((((~(cb >> (ntz + 2))) << 1) + 1) << (ntz + 1));
                                }
                            }
                            formatter.WriteByte(cb);
                        }
                    }else
                        for (var i = 0; i < _byteLength; i++)
                            formatter.WriteByte(i < bytes.Length ? bytes[i] : (byte) 0);
                }
            }
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] {(decimal) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue), typeof(decimal))};
            else if (val.TypeHint == Parser.ConstType.Number)
                Data = new[] {(decimal) Convert.ChangeType(val.StringValue, typeof(decimal))};
            else
                throw new NotSupportedException();
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Decimal({_length}, {_precision})";

        public override void ValueFromParam(ClickHouseParameter parameter) => Data = new[] {(decimal) Convert.ChangeType(parameter.Value, typeof(decimal))};

        public override object Value(int currentRow) => Data[currentRow];

        public override long IntValue(int currentRow) => (long) Data[currentRow];

        public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<decimal>().ToArray();

        public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<decimal?>().Select(x => x ?? 0m).ToArray();
    }
}

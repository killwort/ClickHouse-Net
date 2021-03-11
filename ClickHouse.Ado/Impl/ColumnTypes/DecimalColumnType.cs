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
        private readonly uint _length;
        private readonly uint _precision;
        private readonly bool _doubleFallback;

        public DecimalColumnType(uint length, uint precision) {
            _length = length;
            _precision = precision;
            _doubleFallback = _length - precision >= 28;
            if (length <= 9)
                _byteLength = 4;
            else if (length <= 18)
                _byteLength = 8;
            else if (length <= 38)
                _byteLength = 16;
            else if (length <= 76)
                _byteLength = 32;
            else throw new ClickHouseException($"Invalid Decimal length {length}");
        }

        protected decimal[] DataDecimal { get; set; }
        protected double[] DataDouble { get; set; }

        public override int Rows => _doubleFallback ? DataDouble?.Length ?? 0 : DataDecimal?.Length ?? 0;
        internal override Type CLRType => _doubleFallback ? typeof(double) : typeof(decimal);

        internal override void Read(ProtocolFormatter formatter, int rows) {
            if (_doubleFallback)
                DataDouble = new double[rows];
            else
                DataDecimal = new decimal[rows];
            var bytes = formatter.ReadBytes(rows * _byteLength);
            if (_byteLength <= 8) {
                var exponent = (decimal) Math.Pow(10, (int) _precision);
                for (var i = 0; i < rows; i++)
                    if (_byteLength == 4) {
                        DataDecimal[i] = BitConverter.ToInt32(bytes, i * _byteLength) / exponent;
                    } else if (_byteLength == 8) {
                        DataDecimal[i] = BitConverter.ToInt64(bytes, i * _byteLength) / exponent;
                    }
            } else {
                var exponent = BigInteger.Pow(10, (int) _precision);
                var exponentExponent = _precision <= 28 ? BigInteger.One : BigInteger.Pow(10, (int) (_precision - 28));
                for (var i = 0; i < rows; i++) {
                    var premultiplied = new BigInteger(bytes.Skip(i * _byteLength).Take(_byteLength).ToArray());
                    if (_doubleFallback) {
                        var result = (double) BigInteger.DivRem(premultiplied, exponent, out var remainder);
                        if (_precision <= 28)
                            result += (double) remainder / (double) exponent;
                        else {
                            result += (double) BigInteger.Divide(remainder, exponentExponent) / (double) BigInteger.Divide(exponent, exponentExponent);
                        }

                        DataDouble[i] = result;
                    } else {
                        var result = (decimal) BigInteger.DivRem(premultiplied, exponent, out var remainder);
                        if (_precision <= 28)
                            result += (decimal) remainder / (decimal) exponent;
                        else {
                            result += (decimal) BigInteger.Divide(remainder, exponentExponent) / (decimal) BigInteger.Divide(exponent, exponentExponent);
                        }

                        DataDecimal[i] = result;
                    }
                }
            }
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            if (_byteLength <= 8) {
                var exponent = (decimal) Math.Pow(10, (int) _precision);
                foreach (var d in DataDecimal) {
                    var premultiplied = new BigInteger(d * exponent);
                    if (_byteLength == 4) {
                        formatter.WriteBytes(BitConverter.GetBytes((int) premultiplied));
                    } else if (_byteLength == 8) {
                        formatter.WriteBytes(BitConverter.GetBytes((long) premultiplied));
                    }
                }
            } else {
                if (_doubleFallback) {
                    var doubleExponent = Math.Pow(10, _precision);
                    foreach (var d in DataDouble) {
                        var premultiplied = new BigInteger(d * doubleExponent);
                        var filler = premultiplied < 0 ? (byte) 0xff : (byte) 0;
                        var bytes = premultiplied.ToByteArray();
                        for (var i = 0; i < _byteLength; i++)
                            formatter.WriteByte(i < bytes.Length ? bytes[i] : filler);
                    }
                } else {
                    var exponent = _precision <= 28 ? BigInteger.One : BigInteger.Pow(10, (int) _precision - 28);
                    var decimalExponent = (decimal) Math.Pow(10, Math.Min(_precision, 28));
                    foreach (var d in DataDecimal) {
                        var premultiplied = new BigInteger(d * decimalExponent) * exponent;
                        var filler = premultiplied < 0 ? (byte) 0xff : (byte) 0;
                        var bytes = premultiplied.ToByteArray();
                        for (var i = 0; i < _byteLength; i++)
                            formatter.WriteByte(i < bytes.Length ? bytes[i] : filler);
                    }
                }
            }
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (_doubleFallback) {
                if (val.TypeHint == Parser.ConstType.String)
                    DataDouble = new[] {(double) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue), typeof(double))};
                else if (val.TypeHint == Parser.ConstType.Number)
                    DataDouble = new[] {(double) Convert.ChangeType(val.StringValue, typeof(double))};
                else
                    throw new NotSupportedException();
            } else {
                if (val.TypeHint == Parser.ConstType.String)
                    DataDecimal = new[] {(decimal) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue), typeof(decimal))};
                else if (val.TypeHint == Parser.ConstType.Number)
                    DataDecimal = new[] {(decimal) Convert.ChangeType(val.StringValue, typeof(decimal))};
                else
                    throw new NotSupportedException();
            }
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Decimal({_length}, {_precision})";

        public override void ValueFromParam(ClickHouseParameter parameter) {
            if (_doubleFallback)
                DataDouble = new[] {(double) Convert.ChangeType(parameter.Value, typeof(double))};
            else
                DataDecimal = new[] {(decimal) Convert.ChangeType(parameter.Value, typeof(decimal))};
        }

        public override object Value(int currentRow) => _doubleFallback ? (object) DataDouble[currentRow] : DataDecimal[currentRow];

        public override long IntValue(int currentRow) => _doubleFallback ? (long) DataDouble[currentRow] : (long) DataDecimal[currentRow];

        public override void ValuesFromConst(IEnumerable objects) {
            if (_doubleFallback)
                DataDouble = objects.Cast<double>().ToArray();
            else
                DataDecimal = objects.Cast<decimal>().ToArray();
        }

        public override void NullableValuesFromConst(IEnumerable objects) {
            if (_doubleFallback)
                DataDouble = objects.Cast<double?>().Select(x => x ?? 0.0).ToArray();
            else
                DataDecimal = objects.Cast<decimal?>().Select(x => x ?? 0m).ToArray();
        }
    }
}

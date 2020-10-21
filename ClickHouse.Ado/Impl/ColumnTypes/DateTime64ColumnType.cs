using System;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
using Buffer = System.Buffer;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class DateTime64ColumnType : DateColumnType {
        private static readonly DateTime UnixTimeBase = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static readonly double MaxUnixTimeSeconds = (DateTime.MaxValue - UnixTimeBase).TotalSeconds;
        private readonly int _precision;
        private readonly string _tz;

        public DateTime64ColumnType(int precision, string tz) {
            _precision = precision;
            _tz = tz;
        }

        public DateTime64ColumnType(DateTime[] data) : base(data) { }

        public override int Rows => Data?.Length ?? 0;

        internal override void Read(ProtocolFormatter formatter, int rows) {
#if CLASSIC_FRAMEWORK
            var itemSize = sizeof(ulong);
#else
            var itemSize = Marshal.SizeOf<ulong>();
#endif
            var bytes = formatter.ReadBytes(itemSize * rows);
            var xdata = new ulong[rows];
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            var divisor = Math.Pow(10, -_precision);
            Data = xdata.Select(x => ParseValue(x, divisor)).ToArray();
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            var multiplier = Math.Pow(10, _precision);
            foreach (var d in Data)
                formatter.WriteBytes(BitConverter.GetBytes((ulong) ((d - UnixTimeBase).TotalSeconds * multiplier)));
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) {
            if (string.IsNullOrEmpty(_tz) || usageIntent == ClickHouseTypeUsageIntent.ColumnInfo)
                return $"DateTime64({_precision})";
            return $"DateTime64({_precision}, '{ProtocolFormatter.EscapeStringValue(_tz)}')";
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] {
                    DateTime.ParseExact(
                        ProtocolFormatter.UnescapeStringValue(val.StringValue),
                        new[] {
                            "yyyy-MM-dd HH:mm:ss",
                            "yyyy-MM-dd HH:mm:ss.f",
                            "yyyy-MM-dd HH:mm:ss.ff",
                            "yyyy-MM-dd HH:mm:ss.fff",
                            "yyyy-MM-dd HH:mm:ss.ffff",
                            "yyyy-MM-dd HH:mm:ss.fffff",
                            "yyyy-MM-dd HH:mm:ss.ffffff",
                            "yyyy-MM-dd HH:mm:ss.fffffff"
                        },
                        null,
                        DateTimeStyles.AssumeUniversal
                    )
                };
            else
                throw new InvalidCastException("Cannot convert numeric value to DateTime.");
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            if (parameter.DbType == DbType.Date || parameter.DbType == DbType.DateTime || parameter.DbType == DbType.DateTime2 || parameter.DbType == DbType.DateTimeOffset)
                Data = new[] {(DateTime) Convert.ChangeType(parameter.Value, typeof(DateTime))};
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to DateTime.");
        }

        private DateTime ParseValue(ulong value, double divisor) {
            var dividedValue = value * divisor;

            if (dividedValue > MaxUnixTimeSeconds)
                return DateTime.MinValue;

            return UnixTimeBase.AddSeconds(dividedValue);
        }
    }
}

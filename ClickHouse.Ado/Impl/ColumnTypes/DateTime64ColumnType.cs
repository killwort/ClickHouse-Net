#pragma warning disable CS0618

using System;
using System.Collections;
#if !NETCOREAPP11
using System.Data;
#endif
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Utils;
using Buffer = System.Buffer;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class DateTime64ColumnType : DateColumnType
    {
        internal const byte DefaultPrecision = 3;
        private const byte CLRDefaultPrecision = 7;

        public DateTime64ColumnType()
        {
        }

        public DateTime64ColumnType(DateTime[] data) : base(data)
        {
        }

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
#if FRAMEWORK20 || FRAMEWORK40 || FRAMEWORK45
            var itemSize = sizeof(ulong);
#else
            var itemSize = Marshal.SizeOf<ulong>();
#endif
            var bytes = formatter.ReadBytes(itemSize * rows);
            var xdata = new ulong[rows];
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            Data = xdata.Select(x => UnixTimeBase.AddTicks((long)MathUtils.ShiftDecimalPlaces(x, CLRDefaultPrecision - DefaultPrecision))).ToArray();
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
                formatter.WriteBytes(BitConverter.GetBytes((ulong)((d - UnixTimeBase).TotalMilliseconds)));
        }

        public override string AsClickHouseType()
        {
            return $"DateTime64({DefaultPrecision})";
        }

        public override int Rows => Data?.Length ?? 0;

        public override void ValueFromConst(Parser.ValueType val)
        {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] { DateTime.ParseExact(ProtocolFormatter.UnescapeStringValue(val.StringValue), "yyyy-MM-dd HH:mm:ss.fff", null, DateTimeStyles.AssumeUniversal) };
            else
                throw new InvalidCastException("Cannot convert numeric value to DateTime.");
        }
        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            if (parameter.DbType == DbType.Date || parameter.DbType == DbType.DateTime
#if !NETCOREAPP11
                || parameter.DbType == DbType.DateTime2 || parameter.DbType == DbType.DateTimeOffset
#endif
                )
                Data = new[] { (DateTime)Convert.ChangeType(parameter.Value, typeof(DateTime)) };
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to DateTime.");
        }
    }
}
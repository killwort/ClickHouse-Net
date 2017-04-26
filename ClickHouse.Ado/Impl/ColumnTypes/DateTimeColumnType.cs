using System;
#if !NETCOREAPP11
using System.Data;
#endif
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using ClickHouse.Ado.Impl.ATG.Insert;
using Buffer = System.Buffer;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class DateTimeColumnType : DateColumnType
    {
        private static readonly DateTime UnixTimeBase = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTimeColumnType()
        {
        }

        public DateTimeColumnType(DateTime[] data) : base(data)
        {
        }

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            var itemSize = Marshal.SizeOf<uint>();
            var bytes = formatter.ReadBytes(itemSize * rows);
            var xdata = new uint[rows];
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            Data = xdata.Select(x => UnixTimeBase.AddSeconds(x)).ToArray();
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
                formatter.WriteBytes(BitConverter.GetBytes((uint)((d - UnixTimeBase).TotalSeconds)));
        }

        public override string AsClickHouseType()
        {
            return "DateTime";
        }

        public override int Rows => Data?.Length ?? 0;

        public override void ValueFromConst(string value, Parser.ConstType typeHint)
        {
            if (typeHint == Parser.ConstType.String)
                Data = new[] { DateTime.ParseExact(ProtocolFormatter.UnescapeStringValue(value), "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.AssumeUniversal) };
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
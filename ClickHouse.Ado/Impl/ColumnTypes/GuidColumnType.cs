namespace ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Linq;
    using System.Runtime.InteropServices;
    using ATG.Insert;
    using Buffer = System.Buffer;

    /// <summary> UUID column type </summary>
    internal class GuidColumnType : ColumnType
    {
        internal const string UuidColumnTypeName = "UUID";
        
        public Guid[] Data { get; protected set; }
        
        public override int Rows => Data?.Length ?? 0;

        internal override Type CLRType => typeof(Guid);

        public GuidColumnType()
        {
            
        }

        public GuidColumnType(Guid[] data)
        {
            Data = data;
        }
        
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            var itemSize = Marshal.SizeOf(typeof(Guid));
            var bytes = formatter.ReadBytes(itemSize * rows);
            var xdata = new Guid[rows];
            for (var i = 0; i < rows; i++) {
                var offset = itemSize * i;
                xdata[i] = new Guid(BitConverter.ToInt32(bytes, offset), BitConverter.ToInt16(bytes, offset + 4), BitConverter.ToInt16(bytes, offset + 6), bytes[offset+8], bytes[offset+9], bytes[offset+10], bytes[offset+11], bytes[offset+12], bytes[offset+13], bytes[offset+14], bytes[offset+15]);
            }
            Data = xdata;
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                    Data = new[]
                           {
                               new Guid(val.StringValue)
                           };
                    break;
                default:
                    throw new InvalidCastException("Cannot convert numeric value to Guid.");
            }
        }

        public override string AsClickHouseType()
        {
            return UuidColumnTypeName;
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            foreach (var d in Data) {
                var guidBytes = d.ToByteArray();
                formatter.WriteBytes(guidBytes, 4, 4);
                formatter.WriteBytes(guidBytes, 2, 2);
                for (var b = 15; b >= 8; b--)
                    formatter.WriteByte(guidBytes[b]);
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            switch (parameter.DbType)
            {
                case DbType.Guid:
                    Data = new[] {(Guid) Convert.ChangeType(parameter.Value, typeof(Guid))};
                    break;
                default:
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Guid.");
            }
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
            Data = objects.Cast<Guid>().ToArray();
        }
    }
}
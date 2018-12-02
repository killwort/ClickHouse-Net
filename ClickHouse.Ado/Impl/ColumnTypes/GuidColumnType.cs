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
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            Data = xdata.ToArray();
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

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            foreach (var d in Data)
            {
                formatter.WriteBytes(d.ToByteArray());
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
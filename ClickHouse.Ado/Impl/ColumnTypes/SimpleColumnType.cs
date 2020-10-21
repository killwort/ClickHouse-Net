#pragma warning disable CS0618

using System;
using System.Collections;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
using Buffer = System.Buffer;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class SimpleColumnType<T> : ColumnType where T : struct {
        public SimpleColumnType() { }

        public SimpleColumnType(T[] data) => Data = data;

        public T[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(T);

        internal override void Read(ProtocolFormatter formatter, int rows) {
#if CLASSIC_FRAMEWORK
            var itemSize = Marshal.SizeOf(typeof(T));
#else
            var itemSize = Marshal.SizeOf<T>();
#endif
            var bytes = formatter.ReadBytes(itemSize * rows);
            Data = new T[rows];
            Buffer.BlockCopy(bytes, 0, Data, 0, itemSize * rows);
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) {
            if (typeof(T) == typeof(double))
                return "Float64";
            if (typeof(T) == typeof(float))
                return "Float32";
            if (typeof(T) == typeof(byte))
                return "UInt8";
            if (typeof(T) == typeof(sbyte))
                return "Int8";

            return typeof(T).Name;
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
#if CLASSIC_FRAMEWORK
            var itemSize = Marshal.SizeOf(typeof(T));
#else
            var itemSize = Marshal.SizeOf<T>();
#endif
            var bytes = new byte[itemSize * rows];
            Buffer.BlockCopy(Data, 0, bytes, 0, itemSize * rows);
            formatter.WriteBytes(bytes);
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] {(T) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue), typeof(T))};
            else if (val.TypeHint == Parser.ConstType.Number)
                Data = new[] {(T) Convert.ChangeType(val.StringValue, typeof(T))};
            else
                throw new NotSupportedException();
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            if (parameter.DbType == DbType.Int16 || parameter.DbType == DbType.Int32 || parameter.DbType == DbType.Int64 || parameter.DbType == DbType.UInt16 || parameter.DbType == DbType.UInt32 ||
                parameter.DbType == DbType.UInt64 || parameter.DbType == DbType.Single || parameter.DbType == DbType.Decimal || parameter.DbType == DbType.Double)
                Data = new[] {(T) Convert.ChangeType(parameter.Value, typeof(T))};
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to {typeof(T).Name}.");
        }

        public override object Value(int currentRow) => Data[currentRow];

        public override long IntValue(int currentRow) => Convert.ToInt64(Data[currentRow]);

        public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<T>().ToArray();

        public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<T?>().Select(x => x ?? (T) Activator.CreateInstance(typeof(T))).ToArray();
    }
}

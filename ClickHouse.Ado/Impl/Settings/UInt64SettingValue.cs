using System;

namespace ClickHouse.Ado.Impl.Settings {
    internal class UInt64SettingValue : SettingValue {
        public UInt64SettingValue(ulong value) => Value = value;

        public ulong Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => formatter.WriteUInt((long) Value);

        internal override T As<T>() {
            if (typeof(T) != typeof(ulong)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}
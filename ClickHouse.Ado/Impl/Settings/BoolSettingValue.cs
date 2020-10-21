using System;

namespace ClickHouse.Ado.Impl.Settings {
    internal class BoolSettingValue : SettingValue {
        public BoolSettingValue(bool value) => Value = value;

        public bool Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => formatter.WriteUInt(Value ? 1L : 0L);

        internal override T As<T>() {
            if (typeof(T) != typeof(bool)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}
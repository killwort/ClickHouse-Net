using System;

namespace ClickHouse.Ado.Impl.Settings {
    internal class EnumSettingValue<T> : SettingValue where T : struct {
        public EnumSettingValue(T value) => Value = value;

        public T Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => formatter.WriteUInt((long) Convert.ChangeType(Value, typeof(int)));

        internal override TX As<TX>() {
            if (typeof(TX) != typeof(T)) throw new InvalidCastException();
            return (TX) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}
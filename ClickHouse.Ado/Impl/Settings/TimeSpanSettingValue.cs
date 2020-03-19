using System;

namespace ClickHouse.Ado.Impl.Settings {
    internal class TimeSpanSettingValue : SettingValue {
        public TimeSpanSettingValue(int seconds) => Value = TimeSpan.FromSeconds(seconds);

        public TimeSpanSettingValue(TimeSpan value) => Value = value;

        public TimeSpan Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => formatter.WriteUInt((long) Value.TotalSeconds);

        internal override T As<T>() {
            if (typeof(T) != typeof(TimeSpan)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}
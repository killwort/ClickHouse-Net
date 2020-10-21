using System;

namespace ClickHouse.Ado.Impl.Settings {
    internal class TimeSpanMsSettingValue : TimeSpanSettingValue {
        public TimeSpanMsSettingValue(int milliseconds) : base(TimeSpan.FromMilliseconds(milliseconds)) { }

        public TimeSpanMsSettingValue(TimeSpan value) : base(value) { }

        protected internal override void Write(ProtocolFormatter formatter) => formatter.WriteUInt((long) Value.TotalMilliseconds);
    }
}
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings; 

internal class TimeSpanMsSettingValue : TimeSpanSettingValue {
    public TimeSpanMsSettingValue(int milliseconds) : base(TimeSpan.FromMilliseconds(milliseconds)) { }

    public TimeSpanMsSettingValue(TimeSpan value) : base(value) { }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteUInt((long)Value.TotalMilliseconds, cToken);
}
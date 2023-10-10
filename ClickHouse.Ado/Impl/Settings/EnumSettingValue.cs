using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings;

internal class EnumSettingValue<T> : SettingValue where T : struct {
    public EnumSettingValue(T value) => Value = value;

    public T Value { get; set; }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteUInt((long)Convert.ChangeType(Value, typeof(int)), cToken);

    internal override TX As<TX>() {
        if (typeof(TX) != typeof(T)) throw new InvalidCastException();
        return (TX)(object)Value;
    }

    internal override object AsValue() => Value;
}
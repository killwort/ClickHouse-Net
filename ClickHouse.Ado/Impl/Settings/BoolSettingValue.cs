using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings;

internal class BoolSettingValue : SettingValue {
    public BoolSettingValue(bool value) => Value = value;

    public bool Value { get; set; }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteUInt(Value ? 1L : 0L, cToken);

    internal override T As<T>() {
        if (typeof(T) != typeof(bool)) throw new InvalidCastException();
        return (T)(object)Value;
    }

    internal override object AsValue() => Value;
}
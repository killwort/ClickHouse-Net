using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings;

internal class StringSettingValue : SettingValue {
    public StringSettingValue(string value) => Value = value;

    public string Value { get; set; }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteString(Value, cToken);

    internal override T As<T>() {
        if (typeof(T) != typeof(string)) throw new InvalidCastException();
        return (T)(object)Value;
    }

    internal override object AsValue() => Value;
}
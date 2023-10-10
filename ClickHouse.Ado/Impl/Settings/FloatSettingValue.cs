using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings; 

internal class FloatSettingValue : SettingValue {
    public FloatSettingValue(float value) => Value = value;

    public float Value { get; set; }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteString(Value.ToString(CultureInfo.InvariantCulture), cToken);

    internal override T As<T>() {
        if (typeof(T) != typeof(float)) throw new InvalidCastException();
        return (T)(object)Value;
    }

    internal override object AsValue() => Value;
}
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings; 

internal class UInt64SettingValue : SettingValue {
    public UInt64SettingValue(ulong value) => Value = value;

    public ulong Value { get; set; }

    protected internal override Task Write(ProtocolFormatter formatter, CancellationToken cToken) => formatter.WriteUInt((long)Value, cToken);

    internal override T As<T>() {
        if (typeof(T) != typeof(ulong)) throw new InvalidCastException();
        return (T)(object)Value;
    }

    internal override object AsValue() => Value;
}
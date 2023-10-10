using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings;

internal abstract class SettingValue {
    protected internal abstract Task Write(ProtocolFormatter formatter, CancellationToken cToken);

    internal abstract T As<T>();

    internal abstract object AsValue();
}
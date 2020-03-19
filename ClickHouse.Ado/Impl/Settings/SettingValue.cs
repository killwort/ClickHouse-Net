namespace ClickHouse.Ado.Impl.Settings {
    internal abstract class SettingValue {
        protected internal abstract void Write(ProtocolFormatter formatter);

        internal abstract T As<T>();

        internal abstract object AsValue();
    }
}
namespace ClickHouse.Ado.Impl.Data {
    internal enum DistributedProductMode {
        Deny = 0,

        /// Запретить
        Local,

        /// Конвертировать в локальный запрос
        Global,

        /// Конвертировать в глобальный запрос
        Allow /// Разрешить
    }
}
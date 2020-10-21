namespace ClickHouse.Ado.Impl.Data {
    internal enum GlobalSubqueriesMethod {
        Push = 0,

        /// Отправлять данные подзапроса на все удалённые серверы.
        Pull = 1 /// Удалённые серверы будут скачивать данные подзапроса с сервера-инициатора.
    }
}
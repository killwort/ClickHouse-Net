namespace ClickHouse.Ado.Impl.Data {
    internal enum ServerMessageType {
        Hello = 0,

        /// Имя, версия, ревизия.
        Data = 1,

        /// Блок данных со сжатием или без.
        Exception = 2,

        /// Исключение во время обработки запроса.
        Progress = 3,

        /// Прогресс выполнения запроса: строк считано, байт считано.
        Pong = 4,

        /// Ответ на Ping.
        EndOfStream = 5,

        /// Все пакеты были переданы.
        ProfileInfo = 6,

        /// Пакет с профайлинговой информацией.
        Totals = 7,

        /// Блок данных с тотальными значениями, со сжатием или без.
        Extremes = 8,

        /// Блок данных с минимумами и максимумами, аналогично.
        TablesStatusReposnse = 9,

        /// Ответ на TablesStatus.
        Log = 10,

        /// Логи сервера.
        TableColumns = 11 /// Структура таблицы.
    }
}
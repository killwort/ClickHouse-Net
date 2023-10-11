namespace ClickHouse.Ado.Impl.Data;

internal enum ServerMessageType {
    /// <summary>
    ///     Имя, версия, ревизия.
    /// </summary>
    Hello = 0,

    /// <summary>
    ///     Блок данных со сжатием или без.
    /// </summary>
    Data = 1,

    /// <summary>
    ///     Исключение во время обработки запроса.
    /// </summary>
    Exception = 2,

    /// <summary>
    ///     Прогресс выполнения запроса: строк считано, байт считано.
    /// </summary>
    Progress = 3,

    /// <summary>
    ///     Ответ на Ping.
    /// </summary>
    Pong = 4,

    /// <summary>
    ///     Все пакеты были переданы.
    /// </summary>
    EndOfStream = 5,

    /// <summary>
    ///     Пакет с профайлинговой информацией.
    /// </summary>
    ProfileInfo = 6,

    /// <summary>
    ///     Блок данных с тотальными значениями, со сжатием или без.
    /// </summary>
    Totals = 7,

    /// <summary>
    ///     Блок данных с минимумами и максимумами, аналогично.
    /// </summary>
    Extremes = 8,

    /// <summary>
    ///     Ответ на TablesStatus.
    /// </summary>
    TablesStatusReposnse = 9,

    /// <summary>
    ///     Логи сервера.
    /// </summary>
    Log = 10,

    /// <summary>
    ///     Структура таблицы.
    /// </summary>
    TableColumns = 11
}

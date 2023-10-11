namespace ClickHouse.Ado.Impl.Data;

internal enum ClientMessageType {
    /// <summary>
    ///     Имя, версия, ревизия, БД по-умолчанию.
    /// </summary>
    Hello = 0,

    /// <summary>
    ///     Идентификатор запроса, настройки на отдельный запрос, информация, до какой стадии исполнять запрос, использовать ли
    ///     сжатие, текст запроса (без данных для INSERT-а).
    /// </summary>
    Query = 1,

    /// <summary>
    ///     Блок данных со сжатием или без.
    /// </summary>
    Data = 2,

    /// <summary>
    ///     Отменить выполнение запроса.
    /// </summary>
    Cancel = 3,

    /// <summary>
    ///     Проверка живости соединения с сервером.
    /// </summary>
    Ping = 4
}

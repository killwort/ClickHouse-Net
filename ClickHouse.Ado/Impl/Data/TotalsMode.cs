namespace ClickHouse.Ado.Impl.Data;

internal enum TotalsMode {
    /// <summary>
    ///     Считать HAVING по всем прочитанным строкам;
    ///     включая не попавшие в max_rows_to_group_by
    ///     и не прошедшие HAVING после группировки.
    /// </summary>
    BeforeHaving = 0,

    /// <summary>
    ///     Считать по всем строкам, кроме не прошедших HAVING;
    ///     то есть, включать в TOTALS все строки, не прошедшие max_rows_to_group_by.
    /// </summary>
    AfterHavingInclusive = 1,

    /// <summary>
    ///     Включать только строки, прошедшие и max_rows_to_group_by, и HAVING.
    /// </summary>
    AfterHavingExclusive = 2,

    /// <summary>
    ///     Автоматически выбирать между INCLUSIVE и EXCLUSIVE.
    /// </summary>
    AfterHavingAuto = 3
}

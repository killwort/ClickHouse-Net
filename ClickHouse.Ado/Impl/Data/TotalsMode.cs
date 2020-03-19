namespace ClickHouse.Ado.Impl.Data {
    internal enum TotalsMode {
        BeforeHaving = 0,

        /// Считать HAVING по всем прочитанным строкам;
        /// включая не попавшие в max_rows_to_group_by
        /// и не прошедшие HAVING после группировки.
        AfterHavingInclusive = 1,

        /// Считать по всем строкам, кроме не прошедших HAVING;
        /// то есть, включать в TOTALS все строки, не прошедшие max_rows_to_group_by.
        AfterHavingExclusive = 2,

        /// Включать только строки, прошедшие и max_rows_to_group_by, и HAVING.
        AfterHavingAuto = 3 /// Автоматически выбирать между INCLUSIVE и EXCLUSIVE,
    }
}
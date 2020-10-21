namespace ClickHouse.Ado.Impl.Data {
    internal enum QueryProcessingStage {
        FetchColumns = 0,

        /// Только прочитать/прочитаны указанные в запросе столбцы.
        WithMergeableState = 1,

        /// До стадии, когда результаты обработки на разных серверах можно объединить.
        Complete = 2 /// Полностью.
    }
}
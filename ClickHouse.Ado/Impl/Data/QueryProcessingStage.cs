namespace ClickHouse.Ado.Impl.Data;

internal enum QueryProcessingStage {
    FetchColumns = 0,
    WithMergeableState = 1,
    Complete = 2
}

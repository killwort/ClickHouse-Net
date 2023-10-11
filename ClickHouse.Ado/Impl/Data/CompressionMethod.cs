namespace ClickHouse.Ado.Impl.Data;

internal enum CompressionMethod {
    QuickLz = 0,
    Lz4 = 1,
    Lz4Hc = 2,
    Zstd = 3
}

namespace ClickHouse.Ado.Impl.Data {
    internal enum CompressionMethod {
        QuickLz = 0,
        Lz4 = 1,
        Lz4Hc = 2,

        /// Формат такой же, как у LZ4. Разница только при сжатии.
        Zstd = 3 /// Экспериментальный алгоритм: https://github.com/Cyan4973/zstd
    }
}
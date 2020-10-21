using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.Compress {
    internal abstract class Compressor {
        public abstract CompressionMethod Method { get; }
        public abstract Stream BeginCompression(Stream baseStream);
        public abstract void EndCompression();
        public abstract Stream BeginDecompression(Stream baseStream);
        public abstract void EndDecompression();

        public static Compressor Create(ClickHouseConnectionSettings settings) {
            switch ((settings.Compressor ?? "").ToLower()) {
                case "zstd":
                    throw new NotSupportedException();
                //Actually server doesn't interpret this well. Maybe ZSTD implementation is slightly different?
#if false
                    return new ZstdCompressor();
#endif
                case "lz4hc":
                    throw new NotSupportedException();
                    //Actually server doesn't interpret this well. Maybe LZ4HC implementation is slightly different?
                    return new Lz4Compressor(true, settings);
                case "lz4":
                default:
                    return new Lz4Compressor(false, settings);
            }
        }
    }
}
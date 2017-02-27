using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.Compress
{
    abstract class Compressor
    {
        public abstract Stream BeginCompression(Stream baseStream);
        public abstract void EndCompression();
        public abstract Stream BeginDecompression(Stream baseStream);
        public abstract void EndDecompression();

        public static Compressor Create(string compressor)
        {
            switch ((compressor??"").ToLower())
            {
                case "zstd":
                    throw new NotImplementedException();
                    //Actually server doesn't interpret this well. Maybe ZSTD implementation is slightly different?
#if false
                    return new ZstdCompressor();
#endif
                case "lz4hc":
                    throw new NotImplementedException();
                    //Actually server doesn't interpret this well. Maybe LZ4HC implementation is slightly different?
                    return new Lz4Compressor(true);
                case "lz4":
                default:
                    return new Lz4Compressor(false);
            }
        }

        public abstract CompressionMethod Method { get; }
    }
}

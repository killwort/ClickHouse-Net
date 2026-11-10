using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.Compress;

internal abstract class Compressor {
    public abstract CompressionMethod Method { get; }
    public abstract Stream BeginCompression(Stream baseStream);
    public abstract void EndCompression();
    public abstract Stream BeginDecompression(Stream baseStream);
    public abstract void EndDecompression();

    public static Compressor Create(ClickHouseConnectionSettings settings)
    {
        return new HashingCompressor(settings);
    }
}
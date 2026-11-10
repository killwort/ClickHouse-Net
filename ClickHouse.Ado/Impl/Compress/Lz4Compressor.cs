using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;
using K4os.Compression.LZ4;

namespace ClickHouse.Ado.Impl.Compress;

internal class Lz4Compressor : ICompressionAlgorithm {
    public static readonly byte[] Header = { 0x82 };

    private readonly bool _useHc;

    public Lz4Compressor(bool useHc) => _useHc = useHc;

    public byte[] Compress(MemoryStream uncompressed) {
        var output = new MemoryStream();
        output.Write(Header, 0, Header.Length);
        var compressed = new byte[LZ4Codec.MaximumOutputSize((int)uncompressed.Length)];
        var compressedLength = LZ4Codec.Encode(uncompressed.ToArray(),  0, (int)uncompressed.Length, compressed, 0 ,compressed.Length);
        output.Write(BitConverter.GetBytes(compressedLength + 9), 0, 4);
        output.Write(BitConverter.GetBytes(uncompressed.Length), 0, 4);
        output.Write(compressed, 0, compressedLength);
        return output.ToArray();
    }

    public byte[] Decompress(byte[] header, Stream compressed, out UInt128 compressedHash) {
        if (header[0] != Header[0])
            throw new FormatException($"Invalid header value {header[0]}");

        var compressedSize = BitConverter.ToInt32(header, 1);
        var uncompressedSize = BitConverter.ToInt32(header, 5);
        var read = 0;
        compressedSize -= header.Length;
        var compressedBytes = new byte[compressedSize + header.Length];
        Array.Copy(header, 0, compressedBytes, 0, header.Length);
        do {
            read += compressed.Read(compressedBytes, header.Length + read, compressedSize - read);
        } while (read < compressedSize);

        compressedHash = ClickHouseCityHash.CityHash128(compressedBytes);
        var uncompressed = new byte[uncompressedSize];
        LZ4Codec.Decode(compressedBytes, header.Length, compressedSize, uncompressed, 0, uncompressedSize);
        return uncompressed;
    }
}

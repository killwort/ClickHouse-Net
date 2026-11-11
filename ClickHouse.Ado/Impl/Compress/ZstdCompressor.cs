using System;
using System.IO;

namespace ClickHouse.Ado.Impl.Compress
{
    class ZstdCompressor : ICompressionAlgorithm
    {
        public static readonly byte[] Header =
        {
            0x90
        };

        public byte[] Compress(MemoryStream uncompressed)
        {
            MemoryStream output = new MemoryStream();
            output.Write(Header, 0, Header.Length);
            var comp = new ZstdSharp.Compressor();
            var compressed = comp.Wrap(uncompressed.ToArray());
            output.Write(BitConverter.GetBytes(compressed.Length + 9), 0, 4);
            output.Write(BitConverter.GetBytes(uncompressed.Length), 0, 4);
            output.Write(compressed.ToArray(), 0, compressed.Length);
            return output.ToArray();
        }

        public byte[] Decompress(byte[] header, Stream compressed, out UInt128 compressedHash)
        {
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
            var decomp = new ZstdSharp.Decompressor();
            decomp.Unwrap(compressedBytes, header.Length, compressedSize, uncompressed, 0, uncompressedSize);
            return uncompressed;
        }
    }
}

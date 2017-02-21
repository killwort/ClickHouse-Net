using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;
using LZ4;

namespace ClickHouse.Ado.Impl.Compress
{
    class Lz4Compressor : HashingCompressor
    {
        public Lz4Compressor(bool useHc)
        {
            _useHc = useHc;
        }

        private readonly bool _useHc;


        private static readonly byte[] Header =
        {
            0x82
        };
        protected override byte[] Compress(MemoryStream uncompressed)
        {
            MemoryStream output = new MemoryStream();
            output.Write(Header, 0, Header.Length);
            var compressed = _useHc
                ? LZ4Codec.EncodeHC(uncompressed.ToArray(), 0, (int) uncompressed.Length)
                : LZ4Codec.Encode(uncompressed.ToArray(), 0, (int) uncompressed.Length);
            output.Write(BitConverter.GetBytes(compressed.Length + 9), 0, 4);
            output.Write(BitConverter.GetBytes(uncompressed.Length), 0, 4);
            output.Write(compressed, 0, compressed.Length);
            return output.ToArray();
        }

        protected override Stream Decompress(Stream uncompressed)
        {
            var header = new byte[9];
            int read = 0;
            do
            {
                read += uncompressed.Read(header, read, header.Length - read);
            } while (read < header.Length);
            if (header[0] != Header[0])
                throw new FormatException($"Invalid header value {header[0]}");
            var compressedSize = BitConverter.ToInt32(header, 1);
            var uncompressedSize = BitConverter.ToInt32(header, 5);
            read = 0;
            compressedSize -= header.Length;
            var cdata = new byte[compressedSize];
            do
            {
                read += uncompressed.Read(cdata, read, compressedSize - read);
            } while (read < compressedSize);
            return new MemoryStream(LZ4Codec.Decode(cdata, 0, compressedSize, uncompressedSize));
        }

        public override CompressionMethod Method => _useHc ? CompressionMethod.Lz4Hc : CompressionMethod.Lz4;
    }
}
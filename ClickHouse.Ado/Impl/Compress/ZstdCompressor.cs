//This may actually work with ZStdNet from NuGet

#if false
namespace ClickHouse.Ado.Impl.Compress
{
    class ZstdCompressor : HashingCompressor
    {
        private static readonly byte[] Header =
        {
            0x90
        };
        protected override byte[] Compress(MemoryStream uncompressed)
        {
            MemoryStream output = new MemoryStream();
            output.Write(Header, 0, Header.Length);
            var comp = new ZstdNet.Compressor();
            var compressed = comp.Wrap(uncompressed.ToArray());
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
            var decomp = new ZstdNet.Decompressor();
            return new MemoryStream(decomp.Unwrap(cdata, uncompressedSize));
        }

        public override CompressionMethod Method => CompressionMethod.Zstd;

    }
}
#endif

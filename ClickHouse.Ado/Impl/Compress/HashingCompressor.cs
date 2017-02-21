using System;
using System.IO;

namespace ClickHouse.Ado.Impl.Compress
{
    abstract class HashingCompressor : Compressor
    {
        private Stream _baseStream;
        private MemoryStream _uncompressed;

        public override Stream BeginCompression(Stream baseStream)
        {
            _baseStream = baseStream;
            return _uncompressed=new MemoryStream();
        }

        public override void EndCompression()
        {
            var compressed = Compress(_uncompressed);
            var hash = ClickHouseCityHash.CityHash128(compressed);
            _baseStream.Write(BitConverter.GetBytes(hash.Low), 0, 8);
            _baseStream.Write(BitConverter.GetBytes(hash.High), 0, 8);
            _baseStream.Write(compressed, 0, compressed.Length);
        }

        public override Stream BeginDecompression(Stream baseStream)
        {
            _baseStream = baseStream;
            var hashRead = new byte[16];
            int read = 0;
            do
            {
                read += baseStream.Read(hashRead, read, 16 - read);
            } while (read < 16);
            //FIXME: We should check computed hash against hashRead to enforce validity.
            return Decompress(baseStream);
        }
        public override void EndDecompression()
        {
        }
        protected abstract byte[] Compress(MemoryStream uncompressed);
        protected abstract Stream Decompress(Stream uncompressed);
    }
}
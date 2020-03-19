using System;
using System.IO;

namespace ClickHouse.Ado.Impl.Compress {
    internal abstract class HashingCompressor : Compressor {
        private readonly ClickHouseConnectionSettings _settings;
        private Stream _baseStream;
        private MemoryStream _uncompressed;

        protected HashingCompressor(ClickHouseConnectionSettings settings) => _settings = settings;

        public override Stream BeginCompression(Stream baseStream) {
            _baseStream = baseStream;
            return _uncompressed = new MemoryStream();
        }

        public override void EndCompression() {
            var compressed = Compress(_uncompressed);
            var hash = ClickHouseCityHash.CityHash128(compressed);
            _baseStream.Write(BitConverter.GetBytes(hash.Low), 0, 8);
            _baseStream.Write(BitConverter.GetBytes(hash.High), 0, 8);
            _baseStream.Write(compressed, 0, compressed.Length);
        }

        public override Stream BeginDecompression(Stream baseStream) =>
            new ChunkedStream(
                () => {
                    _baseStream = baseStream;
                    var hashRead = new byte[16];
                    var read = 0;
                    do {
                        read += baseStream.Read(hashRead, read, 16 - read);
                    } while (read < 16);

                    var bytes = Decompress(baseStream, out var hash);

                    if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 0) != hash.Low)
                        throw new ClickHouseException("Checksum verification failed.");
                    if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 8) != hash.High)
                        throw new ClickHouseException("Checksum verification failed.");

                    return bytes;
                }
            );

        public override void EndDecompression() { }

        protected abstract byte[] Compress(MemoryStream uncompressed);
        protected abstract byte[] Decompress(Stream compressed, out UInt128 compressedHash);
    }
}
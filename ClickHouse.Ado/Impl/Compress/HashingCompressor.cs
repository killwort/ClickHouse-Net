using System;
using System.IO;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.Compress;

internal class HashingCompressor : Compressor {
    private readonly ClickHouseConnectionSettings _settings;
    private Stream _baseStream;
    private MemoryStream _uncompressed;

    public HashingCompressor(ClickHouseConnectionSettings settings)
    {
        _settings = settings;
        Method = settings.Compressor?.ToLower() switch
        {
            "zstd" => CompressionMethod.Zstd,
            "lz4" => CompressionMethod.Lz4,
            "" => CompressionMethod.Lz4,
            null => CompressionMethod.Lz4,
            _ => throw new ClickHouseException($"Compression algorithm {settings.Compressor} not supported.")
        };
        _compressor = Method switch
        {
            CompressionMethod.Zstd => new ZstdCompressor(),
            CompressionMethod.Lz4 => new Lz4Compressor(false),
            _ => throw new ClickHouseException($"Compression algorithm {Method} not supported.")
        };
    }

    private ICompressionAlgorithm _zstd, _lz4, _compressor;
    private ICompressionAlgorithm GetAlgorithm(byte[] header)
    {
        if (header[0] == ZstdCompressor.Header[0]) return _zstd ??= new ZstdCompressor();
        if (header[0] == Lz4Compressor.Header[0]) return _lz4 ??= new Lz4Compressor(false);
        throw new ClickHouseException($"Compression algorithm {header[0]} not supported.");
    }

    public override CompressionMethod Method { get; }

    public override Stream BeginCompression(Stream baseStream) {
        _baseStream = baseStream;
        return _uncompressed = new MemoryStream();
    }

    public override void EndCompression() {
        var compressed = _compressor.Compress(_uncompressed);
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

                byte[] header=new byte[9];
                read = 0;
                do {
                    read += baseStream.Read(header, read, header.Length - read);
                } while (read < header.Length);

                var bytes = GetAlgorithm(header).Decompress(header, baseStream, out var hash);

                if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 0) != hash.Low)
                    throw new ClickHouseException("Checksum verification failed.");
                if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 8) != hash.High)
                    throw new ClickHouseException("Checksum verification failed.");

                return bytes;
            }
        );

    public override void EndDecompression() { }
}
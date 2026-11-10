using System.IO;

namespace ClickHouse.Ado.Impl.Compress;

interface ICompressionAlgorithm
{
    byte[] Compress(MemoryStream uncompressed);
    byte[] Decompress(byte[] header, Stream compressed, out UInt128 compressedHash);
}
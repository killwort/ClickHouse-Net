using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ClickHouse.Ado.Impl
{
    internal class UnclosableStream : Stream
    {
        private readonly Stream _baseStream;

#if !NETSTANDARD15 && !NETCOREAPP11
		public override void Close()
        {
        }
#endif

		public UnclosableStream(Stream baseStream)
        {
            _baseStream = baseStream;
        }

        public override void Flush()
        {
            _baseStream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _baseStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _baseStream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var rv= _baseStream.Read(buffer, offset, count);
            return rv;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _baseStream.Write(buffer, offset, count);
        }

        public override bool CanRead
        {
            get { return _baseStream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _baseStream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _baseStream.CanWrite; }
        }

        public override long Length
        {
            get { return _baseStream.Length; }
        }

        public override long Position { get { return _baseStream.Position; } set { _baseStream.Position = value; } }

        public Stream BaseStream => _baseStream;
    }
}
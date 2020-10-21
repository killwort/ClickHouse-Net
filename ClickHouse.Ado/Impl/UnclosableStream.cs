using System.IO;

namespace ClickHouse.Ado.Impl {
    internal class UnclosableStream : Stream {
#if CLASSIC_FRAMEWORK
		public override void Close()
        {
        }
#endif

        public UnclosableStream(Stream baseStream) => BaseStream = baseStream;

        public override void Flush() => BaseStream.Flush();

        public override long Seek(long offset, SeekOrigin origin) => BaseStream.Seek(offset, origin);

        public override void SetLength(long value) => BaseStream.SetLength(value);

        public override int Read(byte[] buffer, int offset, int count) {
            var rv = BaseStream.Read(buffer, offset, count);
            return rv;
        }

        public override void Write(byte[] buffer, int offset, int count) => BaseStream.Write(buffer, offset, count);

        public override bool CanRead => BaseStream.CanRead;

        public override bool CanSeek => BaseStream.CanSeek;

        public override bool CanWrite => BaseStream.CanWrite;

        public override long Length => BaseStream.Length;

        public override long Position { get => BaseStream.Position; set => BaseStream.Position = value; }

        public Stream BaseStream { get; }
    }
}

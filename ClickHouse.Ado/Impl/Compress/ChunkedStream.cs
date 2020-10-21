using System;
using System.IO;

namespace ClickHouse.Ado.Impl.Compress {
    internal class ChunkedStream : Stream {
        private readonly Func<byte[]> _nextChunk;
        private MemoryStream _currentBlock;

        public ChunkedStream(Func<byte[]> nextChunk) {
            _nextChunk = nextChunk;
            _currentBlock = new MemoryStream(nextChunk());
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _currentBlock.Length;

        public override long Position { get => _currentBlock.Position; set => throw new NotSupportedException(); }

        public override void Flush() => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count) {
            var rv = _currentBlock.Read(buffer, offset, count);
            if (rv == 0) {
                _currentBlock = new MemoryStream(_nextChunk());
                rv = _currentBlock.Read(buffer, offset, count);
            }

            return rv;
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
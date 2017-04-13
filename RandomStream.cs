using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S3Performance
{
    class RandomStream : Stream
    {
        private int _seed;
        private long _cursor;
        private long _length;

        public RandomStream(int seed, long length)
        {
            _seed = seed;
            _cursor = 0;
            _length = length;
        }
        private static uint GetSeededRandomUint(uint seed)
        {
            ushort z = (ushort)(seed >> 16);
            ushort w = (ushort)(seed & 0xffff);
            z = (ushort)(36969 * (z & 65535) + (z >> 16));
            w = (ushort)(18000 * (w & 65535) + (w >> 16));
            return ((uint)z << 16) + w;
        }

        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                return _length;
            }
        }

        public override long Position
        {
            get
            {
                return _cursor;
            }

            set
            {
                _cursor = value;
            }
        }

        public override void Flush()
        {
            throw new InvalidOperationException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int limit = count;
            if (_length - _cursor < count)
            {
                limit = (int)(_length - _cursor);
            }
            int bytesRead = 0;
            while (bytesRead < limit)
            {
                buffer[offset + bytesRead] = (byte)GetSeededRandomUint((uint)_seed + (uint)_cursor);
                ++_cursor;
                ++bytesRead;
            }
            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    if (offset < 0 || offset > _length) throw new InvalidOperationException();
                    _cursor = offset;
                    break;
                case SeekOrigin.Current:
                    if (_cursor + offset < 0 || _cursor + offset > _length) throw new InvalidOperationException();
                    _cursor += offset;
                    break;
                case SeekOrigin.End:
                    if (_length + offset < 0 || _length + offset > _length) throw new InvalidOperationException();
                    _cursor = _length + offset;
                    break;
                default:
                    throw new NotImplementedException();
            }
            return _cursor;
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidOperationException();
        }

        protected override void Dispose(bool disposing)
        {
            // ignore disposal
        }
    }
}

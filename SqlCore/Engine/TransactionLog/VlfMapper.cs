using System.Buffers;
using System.IO.MemoryMappedFiles;
using SqlCore.Utils;

namespace SqlCore.Engine.TransactionLog
{
    public sealed class VlfMapper : IDisposable
    {
        private readonly MemoryMappedFile _mmf;
        private readonly List<MemoryMappedViewAccessor> _chunks = new();

        private readonly long _vlfOffset;
        private readonly long _vlfSize;
        private readonly long _chunkSize;

        public delegate void LogBlockHandler(Span<byte> logBlock);
        public delegate void LogBlockHeaderHandler(Span<byte> header);
        public delegate void SpanAction(Span<byte> span);

        public VlfMapper(string filePath, long vlfOffset, long vlfSize, long chunkSize = 1L * 1024 * 1024 * 1024)
        {
            long fileSize = new FileInfo(filePath).Length;

            if (vlfOffset < 0 || vlfOffset >= fileSize)
                throw new ArgumentOutOfRangeException(nameof(vlfOffset));

            if (vlfSize <= 0 || vlfOffset + vlfSize > fileSize)
                throw new ArgumentOutOfRangeException(nameof(vlfSize));

            _vlfOffset = vlfOffset;
            _vlfSize = vlfSize;
            _chunkSize = chunkSize;

            _mmf = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.Open,
                null,
                fileSize,
                MemoryMappedFileAccess.ReadWrite);

            long offset = 0;
            while (offset < _vlfSize)
            {
                long size = Math.Min(_chunkSize, _vlfSize - offset);
                _chunks.Add(
                    _mmf.CreateViewAccessor(_vlfOffset + offset, size, MemoryMappedFileAccess.ReadWrite)
                );
                offset += size;
            }
        }

        public unsafe void ReadLogBlock(
            long offsetInVlf,
            int blockSize,
            LogBlockHandler handler)
        {
            if (blockSize < 512 || blockSize > 61440)
                throw new ArgumentOutOfRangeException(nameof(blockSize));

            if (offsetInVlf < 0 || offsetInVlf + blockSize > _vlfSize)
                throw new ArgumentOutOfRangeException(nameof(offsetInVlf));

            int chunkIndex = (int)(offsetInVlf / _chunkSize);
            long offsetInChunk = offsetInVlf % _chunkSize;

            var accessor = _chunks[chunkIndex];
            long remaining = accessor.Capacity - offsetInChunk;

            if (remaining >= blockSize)
            {
                byte* ptr = null;
                var handle = accessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += accessor.PointerOffset;
                    var span = new Span<byte>(ptr + offsetInChunk, blockSize);
                    handler(span);
                }
                finally
                {
                    handle.ReleasePointer();
                }
                return;
            }

            byte[] buffer = new byte[blockSize];

            {
                byte* ptr = null;
                var handle = accessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += accessor.PointerOffset;
                    new Span<byte>(ptr + offsetInChunk, (int)remaining)
                        .CopyTo(buffer);
                }
                finally
                {
                    handle.ReleasePointer();
                }
            }

            var nextAccessor = _chunks[chunkIndex + 1];
            {
                byte* ptr = null;
                var handle = nextAccessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += nextAccessor.PointerOffset;
                    new Span<byte>(ptr, blockSize - (int)remaining)
                        .CopyTo(buffer.AsSpan((int)remaining));
                }
                finally
                {
                    handle.ReleasePointer();
                }
            }

            handler(buffer);
        }

        public unsafe void ReadLogBlockHeader(
            long offsetInVlf,
            LogBlockHeaderHandler handler)
        {
            const int headerSize = 72;

            if (offsetInVlf < 0 || offsetInVlf + headerSize > _vlfSize)
                throw new ArgumentOutOfRangeException(nameof(offsetInVlf));

            int chunkIndex = (int)(offsetInVlf / _chunkSize);
            long offsetInChunk = offsetInVlf % _chunkSize;

            var accessor = _chunks[chunkIndex];
            long remaining = accessor.Capacity - offsetInChunk;

            if (remaining >= headerSize)
            {
                byte* ptr = null;
                var handle = accessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += accessor.PointerOffset;
                    handler(new Span<byte>(ptr + offsetInChunk, headerSize));
                }
                finally
                {
                    handle.ReleasePointer();
                }
                return;
            }

            byte[] buffer = new byte[headerSize];

            {
                byte* ptr = null;
                var handle = accessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += accessor.PointerOffset;
                    new Span<byte>(ptr + offsetInChunk, (int)remaining)
                        .CopyTo(buffer);
                }
                finally
                {
                    handle.ReleasePointer();
                }
            }

            var nextAccessor = _chunks[chunkIndex + 1];
            {
                byte* ptr = null;
                var handle = nextAccessor.SafeMemoryMappedViewHandle;

                handle.AcquirePointer(ref ptr);
                try
                {
                    ptr += nextAccessor.PointerOffset;
                    new Span<byte>(ptr, headerSize - (int)remaining)
                        .CopyTo(buffer.AsSpan((int)remaining));
                }
                finally
                {
                    handle.ReleasePointer();
                }
            }

            handler(buffer);
        }

        public static void PooledBuffer(int size, SpanAction action)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                action(buffer.AsSpan(0, size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public void Dispose()
        {
            foreach (var c in _chunks)
                c.Dispose();

            _mmf.Dispose();
        }
    }
}

using SqlCore.Utils;
using System.Buffers.Binary;

namespace SqlCore.Engine.TransactionLog
{
    public static class LogBlockChecksum
    {
        private static int numOfSectors;
        private const short sectorSize = 512;
        private const int numOfElements = 128;

        public static uint CalculateLogBlockChecksum(Span<byte> logBlockContent)
        {
            numOfSectors = logBlockContent.Length / sectorSize;

            uint[,] pagebuf = new uint[numOfSectors, numOfElements];

            int offset = 0;

            for (int i = 0; i < numOfSectors; i++)
            {
                for (int j = 0; j < numOfElements; j++)
                {
                    pagebuf[i, j] = BinaryPrimitives.ReadUInt32LittleEndian(logBlockContent.Slice(offset, 4));
                    offset += 4;
                }
            }

            uint checksum = 0;
            uint result = 0;

            pagebuf[0, 6] = 0x00000000;

            for (int i = 0; i < numOfSectors; i++)
            {
                result = 0;

                for (int j = 0; j < numOfElements; j++)
                {
                    result ^= pagebuf[i, j];
                }

                checksum ^= Functions.rol(result, numOfSectors - i - 1);

            }

            return checksum;
        }

    }
}

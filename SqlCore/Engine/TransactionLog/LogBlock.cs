using System.Buffers.Binary;

namespace SqlCore.Engine.TransactionLog
{
    public class LogBlock
    {
        public static ushort[] ReadLogBlockSlotArray(ReadOnlySpan<byte> logBlockContent, int offsetSlotArray, int numOfRecords)
        {
            if (logBlockContent == null)
                throw new ArgumentNullException(nameof(logBlockContent));

            if (numOfRecords < 0)
                throw new ArgumentOutOfRangeException(nameof(numOfRecords));

            if (offsetSlotArray < 1 || offsetSlotArray >= logBlockContent.Length)
                throw new ArgumentOutOfRangeException(nameof(offsetSlotArray));

            int requiredBytes = numOfRecords * 2;
            int startOffset = offsetSlotArray - requiredBytes + 1;

            if (startOffset < 0)
                throw new ArgumentOutOfRangeException("Not enough data before offset");

            ushort[] slotArray = new ushort[numOfRecords];

            int offset = offsetSlotArray - 2;

            for (int i = 0; i < numOfRecords; i++)
            {
                slotArray[i] = BinaryPrimitives.ReadUInt16LittleEndian(logBlockContent.Slice(offset, 2));
                offset -= 2;
            }

            return slotArray;
        }
    }
}

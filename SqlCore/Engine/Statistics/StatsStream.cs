using SqlCore.Utils;

namespace SqlCore.Engine.Statistics
{
    public class StatsStream
    {
        public StatsStream(byte[] statsStreamData)
        {
            int currentStatsStreamOffset = 0;
            byte histogramSystemTypeId;

            header.NumOfStatsColumns = BitConverter.ToInt32(statsStreamData, 4);
            header.Checksum = BitConverter.ToInt32(statsStreamData, 16);
            header.StatsStreamSize = BitConverter.ToInt64(statsStreamData, 24);
            header.StatsBlobSize = BitConverter.ToInt64(statsStreamData, 32);

            currentStatsStreamOffset = 40;

            var currentStatsColumnData = new byte[24];

            statsColumns = new StatsColumn[header.NumOfStatsColumns];

            for (int i = 0; i < header.NumOfStatsColumns; i++)
            {
                Array.Copy(statsStreamData, currentStatsStreamOffset + (i * 24), currentStatsColumnData, 0, 24);
                statsColumns[i] = new StatsColumn
                {
                    SystemTypeId = currentStatsColumnData[0],
                    IsNullable = !Functions.IsBitSet(currentStatsColumnData[1], 0),
                    UserTypeId = BitConverter.ToInt32(currentStatsColumnData, 4),
                    MaxLength = BitConverter.ToInt16(currentStatsColumnData, 8),
                    Precision = currentStatsColumnData[10],
                    Scale = currentStatsColumnData[11],
                    CollationId = BitConverter.ToInt32(currentStatsColumnData, 16)
                };
            }

            histogramSystemTypeId = statsColumns[0].SystemTypeId;

            currentStatsStreamOffset += header.NumOfStatsColumns * 24;

            var statsBlobData = new byte[header.StatsBlobSize];
            Array.Copy(statsStreamData, currentStatsStreamOffset, statsBlobData, 0, header.StatsBlobSize);

            statsBlob = new StatsBlob.StatsBlob(statsBlobData, histogramSystemTypeId);
        }

        private static readonly uint[] crc32Table = Functions.GenerateCRC32Table(0xEDB88320);

        public Header header = new Header();
        public StatsColumn[] statsColumns;
        public StatsBlob.StatsBlob statsBlob;

        public class Header
        {
            public int NumOfStatsColumns;
            public int Checksum;
            public long StatsStreamSize;
            public long StatsBlobSize;
        }

        public class StatsColumn
        {
            public byte SystemTypeId;
            public bool IsNullable;
            public int UserTypeId;
            public short MaxLength;
            public byte Precision;
            public byte Scale;
            public int CollationId;
        }

        public static uint CalculateStatsChecksum(byte[] stats_stream)
        {
            stats_stream[16] = stats_stream[17] = stats_stream[18] = stats_stream[19] = 0;

            uint checksum = 0xFFFFFFFF;
            foreach (var b in stats_stream)
            {
                checksum = crc32Table[(checksum ^ b) & 0xFF] ^ (checksum >> 8);
            }
            checksum ^= 0xFFFFFFFF;

            checksum = Functions.BinarySwap(checksum);
            checksum = ~checksum;

            return checksum;
        }
    }
}

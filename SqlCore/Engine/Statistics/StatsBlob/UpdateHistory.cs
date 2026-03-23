using SqlCore.Engine.SqlTypes;

namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class UpdateHistory
    {
        public UpdateHistoryRecord[] updateHistoryRecord;

        public class UpdateHistoryRecord
        {
            public DateTime Updated { get; set; }
            public long RowCount { get; set; }
            public int StepCount { get; set; }
        }

        public UpdateHistory(byte[] scanHistoryData)
        {
            var recordCount = BitConverter.ToInt32(scanHistoryData, 0);
            var recordSize = (scanHistoryData.Length - 4) / recordCount;

            byte[] currentRecordData;

            updateHistoryRecord = new UpdateHistoryRecord[recordCount];

            for (int i = 0; i < recordCount; i++)
            {
                currentRecordData = new byte[recordSize];
                Array.Copy(scanHistoryData, (i * recordSize) + 4, currentRecordData, 0, (int)recordSize);
                updateHistoryRecord[i] = new UpdateHistoryRecord
                {
                    Updated = SqlDateTime.Parse(BitConverter.ToInt64(currentRecordData, 0)),
                    RowCount = BitConverter.ToInt64(currentRecordData, 16),
                    StepCount = BitConverter.ToInt32(currentRecordData, 24)
                };
            }
        }
    }
}

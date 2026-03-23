using System.Text;
using SqlCore.Utils;

namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class Histogram
    {
        private long[] OffsetArray;

        public HistogramStep[] Steps;

        public class HistogramStep
        {
            public string RangeHiKey { get; set; }
            public float RangeRows { get; set; }
            public float EqRows { get; set; }
            public float DistinctRangeRows { get; set; }
            public float AvgRangeRows { get; set; }
        }

        public Histogram(byte[] histogramData,
                         int stepCount,
                         int histogramMinRowSize,
                         byte systemTypeId,
                         float stepNullEQRows)
        {
            OffsetArray = new long[stepCount];

            for (int i = 0; i < stepCount; i++)
            {
                OffsetArray[i] = BitConverter.ToInt64(histogramData, i * 8);
            }

            bool hasNullStep = stepNullEQRows > 0;
            int totalStepCount = stepCount + (hasNullStep ? 1 : 0);

            Steps = new HistogramStep[totalStepCount];

            int stepIndex = 0;
            int offsetIndex = 0;

            if (hasNullStep)
            {
                Steps[0] = new HistogramStep
                {
                    RangeHiKey = "NULL",
                    RangeRows = 0,
                    EqRows = stepNullEQRows,
                    DistinctRangeRows = 0,
                    AvgRangeRows = 1
                };

                stepIndex = 1;
            }

            for (; stepIndex < totalStepCount; stepIndex++, offsetIndex++)
            {
                long start = OffsetArray[offsetIndex];
                long end = (offsetIndex + 1 < OffsetArray.Length)
                    ? OffsetArray[offsetIndex + 1]
                    : histogramData.Length;

                long size = end - start;

                byte[] stepData = new byte[size];
                Array.Copy(histogramData, start, stepData, 0, (int)size);

                bool hasVariableData = Functions.IsBitSet(stepData[0], 5);

                float eqRows = BitConverter.ToSingle(stepData, 4);
                float rangeRows = BitConverter.ToSingle(stepData, 8);
                float avgRangeRows = BitConverter.ToSingle(stepData, 12);

                Steps[stepIndex] = new HistogramStep
                {
                    RangeHiKey = GetRangeHiKey(systemTypeId, hasVariableData, stepData, size, histogramMinRowSize),
                    RangeRows = rangeRows,
                    EqRows = eqRows,
                    DistinctRangeRows = avgRangeRows == 0 ? 0 : rangeRows / avgRangeRows,
                    AvgRangeRows = avgRangeRows
                };
            }
        }

        private string GetRangeHiKey(byte systemTypeId,
                                     bool hasStepVariableData,
                                     byte[] currentStepData,
                                     long currentStepDataSize,
                                     int minRowSize)
        {
            byte[] rangeHiKeyData;
            int rangeHiKeyDataSize;
            int rangeHiKeyOffset;

            if (hasStepVariableData)
            {
                rangeHiKeyOffset = minRowSize + 7;
                rangeHiKeyDataSize = (int)currentStepDataSize - rangeHiKeyOffset;
            }
            else
            {
                rangeHiKeyOffset = 16;
                rangeHiKeyDataSize = (int)currentStepDataSize - rangeHiKeyOffset - 3;
            }
            rangeHiKeyData = new byte[rangeHiKeyDataSize];
            Array.Copy(currentStepData, rangeHiKeyOffset, rangeHiKeyData, 0, (int)rangeHiKeyDataSize);

            if (systemTypeId == 48)
                return rangeHiKeyData.ToString();

            if (systemTypeId == 52)
                return BitConverter.ToInt16(rangeHiKeyData, 0).ToString();

            if (systemTypeId == 56)
                return BitConverter.ToInt32(rangeHiKeyData, 0).ToString();

            if (systemTypeId == 167)
                return Encoding.ASCII.GetString(rangeHiKeyData, 0, rangeHiKeyDataSize);

            return "";
        }
    }
}

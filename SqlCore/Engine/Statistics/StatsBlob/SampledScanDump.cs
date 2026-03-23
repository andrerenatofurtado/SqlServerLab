namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class SampledScanDump
    {
        private long[] OffsetArray;
        public double[][] VectorSamples;

        public SampledScanDump(byte[] sampleScanDumpData, int currentOffset, int numOfVector)
        {
            OffsetArray = new long[numOfVector];
            VectorSamples = new double[numOfVector][];

            for (int i = 0; i < numOfVector; i++)
            {
                OffsetArray[i] = BitConverter.ToInt64(sampleScanDumpData, i * 8);

                int vectorOffset = (int)OffsetArray[i] - currentOffset;

                int numOfBuckets = BitConverter.ToInt32(sampleScanDumpData, vectorOffset);
                vectorOffset += 4;

                VectorSamples[i] = new double[numOfBuckets];

                for (int j = 0; j < numOfBuckets; j++)
                {
                    VectorSamples[i][j] = BitConverter.ToDouble(sampleScanDumpData, vectorOffset);
                    vectorOffset += 8;
                }
            }
        }
    }
}

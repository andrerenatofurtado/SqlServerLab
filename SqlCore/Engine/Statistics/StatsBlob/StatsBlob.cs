using SqlCore.Utils;
using SqlCore.Engine.SqlTypes;

namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class StatsBlob
    {

        public StatsBlob(byte[] statsBlobData, byte histogramSystemTypeId)
        {
            int currentStatsBlobOffset = 0;

            fixedData.metadataHeader.Version = BitConverter.ToInt32(statsBlobData, currentStatsBlobOffset);
            fixedData.metadataHeader.Updated = SqlDateTime.Parse(BitConverter.ToInt64(statsBlobData, currentStatsBlobOffset + 4));
            fixedData.metadataHeader.Rows = BitConverter.ToInt64(statsBlobData, currentStatsBlobOffset + 12);
            fixedData.metadataHeader.RowsSampled = BitConverter.ToInt64(statsBlobData, currentStatsBlobOffset + 20);
            fixedData.metadataHeader.Density = BitConverter.ToSingle(statsBlobData, currentStatsBlobOffset + 28);

            currentStatsBlobOffset = 32;

            fixedData.VectorsDensities = new float[33];

            for (int i = 0; i < 33; i++)
            {
                fixedData.VectorsDensities[i] = BitConverter.ToSingle(statsBlobData, currentStatsBlobOffset);
                currentStatsBlobOffset += sizeof(float);
            }

            fixedData.metadataHeader.StepCount = BitConverter.ToInt32(statsBlobData, currentStatsBlobOffset);
            fixedData.metadataHeader.VectorCount = BitConverter.ToInt32(statsBlobData, currentStatsBlobOffset + 8);
            fixedData.metadataHeader.HistogramMinRowSize = BitConverter.ToInt32(statsBlobData, currentStatsBlobOffset + 12);
            fixedData.metadataHeader.AverageKeyLength = BitConverter.ToSingle(statsBlobData, currentStatsBlobOffset + 16);
            fixedData.metadataHeader.StepNullEQRows = BitConverter.ToSingle(statsBlobData, currentStatsBlobOffset + 24);

            currentStatsBlobOffset += 28;

            fixedData.VectorsAverageLengths = new float[33];

            for (int i = 0; i < 33; i++)
            {
                fixedData.VectorsAverageLengths[i] = BitConverter.ToSingle(statsBlobData, currentStatsBlobOffset);
                currentStatsBlobOffset += sizeof(float);
            }

            byte numOfVariableItens =
                variableData.parseBitMask(
                    statsBlobData[currentStatsBlobOffset], statsBlobData[currentStatsBlobOffset + 1]);

            numOfVariableItens += 1;
            currentStatsBlobOffset += 8;

            variableData.OffsetArray = new long[numOfVariableItens];

            for (int i = 0; i < numOfVariableItens; i++)
            {
                variableData.OffsetArray[i] = BitConverter.ToInt64(statsBlobData, currentStatsBlobOffset);
                currentStatsBlobOffset += sizeof(long);
            }

            byte numOfParsedVariableItens = 0;

            if (variableData.HasHistogram)
            {
                byte[] histogramData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.Histogram =
                    new Histogram(histogramData,
                                  fixedData.metadataHeader.StepCount,
                                  fixedData.metadataHeader.HistogramMinRowSize,
                                  histogramSystemTypeId,
                                  fixedData.metadataHeader.StepNullEQRows);
            }

            if (variableData.HasStringIndex)
            {
                byte[] stringIndexData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.StringIndex =
                    new StringIndex(stringIndexData, false);
            }

            if (variableData.HasUpdateHistory)
            {
                byte[] updateHistoryData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.UpdateHistory = new UpdateHistory(updateHistoryData);
            }

            if (variableData.HasUnfilteredRows)
            {
                byte[] unfilteredRowsData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.UnfilteredRows = BitConverter.ToInt64(unfilteredRowsData);
            }

            if (variableData.HasSampledScanDump)
            {
                byte[] sampledScanDumpData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.SampledScanDump =
                    new SampledScanDump(sampledScanDumpData,
                                            (int)variableData.OffsetArray[numOfParsedVariableItens - 1],
                                            fixedData.metadataHeader.VectorCount);
            }

            if (variableData.HasPageCountSampled)
            {
                byte[] pageCountSampledData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.PageCountSampled = BitConverter.ToInt64(pageCountSampledData);
            }

            if (variableData.HasDensityVector)
            {
                byte[] vectorDensityData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                byte[] vectorAverageKeyData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.DensityVector =
                    new DensityVector(vectorDensityData,
                                        vectorAverageKeyData,
                                            fixedData.metadataHeader.VectorCount);
            }

            if (variableData.HasPersistedSamplePercent)
            {
                byte[] persistedSamplePercentdData = ReadVariableBlock(
                    statsBlobData,
                    variableData.OffsetArray,
                    ref currentStatsBlobOffset,
                    ref numOfParsedVariableItens);

                variableData.PersistedSamplePercent = BitConverter.ToDouble(persistedSamplePercentdData);
            }
        }

        public FixedData fixedData = new FixedData();
        public VariableData variableData = new VariableData();

        public class FixedData
        {
            public MetadataHeader metadataHeader = new MetadataHeader();

            public class MetadataHeader
            {
                public int Version;
                public DateTime Updated;
                public long Rows;
                public long RowsSampled;
                public float Density;
                public int StepCount;
                public int VectorCount;
                public int HistogramMinRowSize;
                public float AverageKeyLength;
                public float StepNullEQRows;
            }

            public float[] VectorsDensities;
            public float[] VectorsAverageLengths;

        }

        public class VariableData
        {
            public bool HasHistogram;
            public bool HasStringIndex;
            public bool HasUpdateHistory;
            public bool HasDensityVector;
            public bool HasSampledScanDump;
            public bool HasUnfilteredRows;
            public bool HasPageCountSampled;
            public bool HasPersistedSamplePercent;

            public long[] OffsetArray;

            public Histogram Histogram;
            public StringIndex StringIndex;
            public UpdateHistory UpdateHistory;
            public SampledScanDump SampledScanDump;
            public DensityVector DensityVector;

            public long UnfilteredRows;
            public long PageCountSampled;
            public double PersistedSamplePercent;

            public byte parseBitMask(byte b1, byte b2)
            {
                byte numOfBitsSet = 0;

                if (Functions.IsBitSet(b1, 4) &&
                    Functions.IsBitSet(b1, 0) &&
                    Functions.IsBitSet(b2, 2))
                {
                    HasHistogram = true;
                    HasUnfilteredRows = true;
                    HasPersistedSamplePercent = true;
                    numOfBitsSet += 3;
                }

                if (Functions.IsBitSet(b2, 1) && Functions.IsBitSet(b2, 0))
                {
                    HasDensityVector = true;
                    numOfBitsSet += 2;
                }

                if (Functions.IsBitSet(b1, 3))
                {
                    HasUpdateHistory = true;
                    numOfBitsSet += 1;
                }

                if (Functions.IsBitSet(b1, 1))
                {
                    HasStringIndex = true;
                    numOfBitsSet += 1;
                }

                if (Functions.IsBitSet(b1, 7) && Functions.IsBitSet(b1, 5))
                {
                    HasSampledScanDump = true;
                    HasPageCountSampled = true;
                    numOfBitsSet += 2;
                }

                return numOfBitsSet;
            }
        }

        private static byte[] ReadVariableBlock(
            byte[] source,
            long[] offsetArray,
            ref int currentOffset,
            ref byte itemIndex)
        {
            long dataSize = offsetArray[itemIndex + 1] - offsetArray[itemIndex];

            byte[] data = new byte[dataSize];
            Array.Copy(source, currentOffset, data, 0, (int)dataSize);

            currentOffset += (int)dataSize;
            itemIndex += 1;

            return data;
        }
    }
}

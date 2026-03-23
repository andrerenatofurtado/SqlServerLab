namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class DensityVector
    {
        public float[] VectorsDensities;
        public float[] VectorsAverageLengths;

        public DensityVector(byte[] vectorDensityData, byte[] vectorAverageKeyData, int numOfVector)
        {
            int numOfVariableVector = numOfVector - 33;

            VectorsDensities = new float[numOfVariableVector];
            VectorsAverageLengths = new float[numOfVariableVector];

            for (int i = 0; i < (numOfVariableVector); i++)
            {
                VectorsDensities[i] = BitConverter.ToSingle(vectorDensityData, i * 4);
                VectorsAverageLengths[i] = BitConverter.ToSingle(vectorAverageKeyData, i * 4);
            }
        }
    }
}

using SqlCore.Engine.Statistics;

namespace StatsStreamParser
{
    public static class Parser
    {
        public static void ParseStatsStream(string outputPath, byte[] statsStreamData)
        {
            StatsStream statsStream = new StatsStream(statsStreamData);

            uint calculatedChecksum = StatsStream.CalculateStatsChecksum(statsStreamData);

            PrinTables.PrintStatsStream(outputPath, statsStream, calculatedChecksum);
        }
    }
}

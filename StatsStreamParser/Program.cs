using SqlCore.Engine;
using SqlCore.Utils;
using StatsStreamParser;

class Program
{
    static void Main()
    {
        try
        {
            Console.Write("Enter the output file path: ");

            string? outputPath = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(outputPath))
            {
                Console.WriteLine("Invalid output file path.");
                return;
            }

            Console.WriteLine("");

            Console.Write("Enter the STATS_STREAM file path: ");

            string? inputPath = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(inputPath))
            {
                Console.WriteLine("Invalid input file path.");
                return;
            }

            string? stats_stream = File.ReadAllText(inputPath);

            if (string.IsNullOrWhiteSpace(stats_stream))
            {
                Console.WriteLine("Invalid STATS_STREAM blob.");
                return;
            }

            byte[] stats_stream_blob = Functions.BlobStringToBytes(stats_stream);

            Parser.ParseStatsStream(outputPath, stats_stream_blob);

        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

}

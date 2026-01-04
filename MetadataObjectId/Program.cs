using SqlCore.Engine;

namespace MetadataObjectId
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Generate next Object ID (metadata)

            GenerateNextMetadataObjectId();
        }

        static void GenerateNextMetadataObjectId()
        {
            Console.Write("Enter a Object ID: ");
            string input = Console.ReadLine();

            Console.WriteLine();

            Console.WriteLine($"Next Object ID: " + Metadata.GenerateNextMetadataObjectId((int.Parse(input))));

            Console.ReadKey();
        }

    }
}

using SqlCore.Engine.Statistics;
using SqlCore.Engine.Statistics.StatsBlob;
using SqlCore.Utils;
using System.Text;

namespace StatsStreamParser
{
    public static class PrinTables
    {
        public static void PrintStatsStream(string outputPath, StatsStream parsedStatsStream, uint calculatedChecksum)
        {
            PrintStatsStreamHeader(outputPath, parsedStatsStream.header, calculatedChecksum);
            PrintStatsColumns(outputPath, parsedStatsStream.statsColumns);
            PrintStatsBlob(outputPath, parsedStatsStream.statsBlob);
        }

        public static void PrintStatsBlob(string outputPath, StatsBlob parsedStatsBlob)
        {
            PrintStatsBlobHeader(outputPath, parsedStatsBlob);
            PrintStatsBlobDensityVector(outputPath, parsedStatsBlob);
            PrintStatsBlobHistogram(outputPath, parsedStatsBlob.variableData.Histogram);
            if (parsedStatsBlob.variableData.HasStringIndex)
            {
                PrintStatsBlobStringIndex(outputPath, parsedStatsBlob.variableData.StringIndex);
            }
            if (parsedStatsBlob.variableData.HasUpdateHistory)
            {
                PrintStatsBlobUpdateHistory(outputPath, parsedStatsBlob.variableData.UpdateHistory);
            }
            if (parsedStatsBlob.variableData.HasSampledScanDump)
            {
                PrintStatsBlobSampledScanDump(outputPath, parsedStatsBlob.variableData.SampledScanDump, parsedStatsBlob.fixedData.metadataHeader.VectorCount);
            }
        }

        public static void PrintStatsStreamHeader(string outputPath, StatsStream.Header statsStreamHeader, uint calculatedChecksum)
        {
            var printTableTitle = "STATS_STREAM HEADER";

            var printTableHeaders = new[] {
                "Num Of Stats Columns",
                "Checksum",
                "Calculated Checksum",
                "Stats Stream Size",
                "Stats Blob Size"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            printTableRows.Add(new[] {
                $"{statsStreamHeader.NumOfStatsColumns}",
                $"0x{statsStreamHeader.Checksum:X}",
                FormatChecksum(calculatedChecksum),
                $"{statsStreamHeader.StatsStreamSize}",
                $"{statsStreamHeader.StatsBlobSize}"
            });

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsColumns(string outputPath, StatsStream.StatsColumn[] statsColumns)
        {
            var printTableTitle = "STATS COLUMNS";

            var printTableHeaders = new[] {
                "System Type ID",
                "Is Nullable",
                "User Type ID",
                "Max Length",
                "Precision",
                "Scale",
                "Collation ID"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            foreach (StatsStream.StatsColumn statsColumn in statsColumns)
            {
                printTableRows.Add(new[] {
                    $"{statsColumn.SystemTypeId}",
                    $"{(statsColumn.IsNullable ? "YES" : "NO")}",
                    $"{statsColumn.UserTypeId}",
                    $"{statsColumn.MaxLength}",
                    $"{statsColumn.Precision}",
                    $"{statsColumn.Scale}",
                    $"{statsColumn.CollationId}"
            });
            }

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobHeader(string outputPath, StatsBlob statsBlob)
        {
            var printTableTitle = "STATS BLOB HEADER";

            var printTableHeaders = new[] {
                "Updated",
                "Rows",
                "Rows Sampled",
                "Steps",
                "Density",
                "Average Key Length",
                "String Index",
                "Unfiltered Rows",
                "Persisted Sample Percent",
                "Page Count Sampled"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            var statsBlobHeader = statsBlob.fixedData.metadataHeader;
            var statsBlobVarData = statsBlob.variableData;

            var stepCount = statsBlobHeader.StepCount +
                (statsBlobHeader.StepNullEQRows > 0 ? 1 : 0);

            printTableRows.Add(new[] {
                $"{statsBlobHeader.Updated}",
                $"{statsBlobHeader.Rows}",
                $"{statsBlobHeader.RowsSampled}",
                $"{stepCount}",
                $"{statsBlobHeader.Density}",
                $"{statsBlobHeader.AverageKeyLength}",
                $"{(statsBlobVarData.HasStringIndex ? "YES" : "NO")}",
                $"{statsBlobVarData.UnfilteredRows}",
                $"{statsBlobVarData.PersistedSamplePercent}",
                $"{(statsBlobVarData.HasPageCountSampled ? statsBlobVarData.PageCountSampled : "")}",
            });

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobDensityVector(string outputPath, StatsBlob statsBlob)
        {
            var printTableTitle = "DENSITY VECTOR";

            var printTableHeaders = new[] {
                "All Density",
                "Average Length"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            var statsBlobHeader = statsBlob.fixedData.metadataHeader;
            var statsBlobVarData = statsBlob.variableData;

            float totalVectorsAverageLength = 0;

            int totalFixedDensityVectors =
                statsBlobHeader.VectorCount < 33 
                    ? statsBlobHeader.VectorCount : 33;

            for (int i = 0; i < totalFixedDensityVectors; i++)
            {
                totalVectorsAverageLength += statsBlob.fixedData.VectorsAverageLengths[i];

                printTableRows.Add(new[] {
                    $"{statsBlob.fixedData.VectorsDensities[i]}",
                    $"{totalVectorsAverageLength}"
            });
            }

            if (statsBlobVarData.HasDensityVector)
            {
                int totalVariableDensityVectors = statsBlobHeader.VectorCount - 33;

                for (int i = 0; i < totalVariableDensityVectors; i++)
                {
                    totalVectorsAverageLength += statsBlobVarData.DensityVector.VectorsAverageLengths[i];

                    printTableRows.Add(new[] {
                    $"{statsBlobVarData.DensityVector.VectorsDensities[i]}",
                    $"{totalVectorsAverageLength}"
                });
                }
            }

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobHistogram(string outputPath, Histogram histogram)
        {
            var printTableTitle = "HISTOGRAM";

            var printTableHeaders = new[] {
                "RANGE_HI_KEY",
                "RANGE_ROWS",
                "EQ_ROWS",
                "DISTINCT_RANGE_ROWS",
                "AVG_RANGE_ROWS"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            foreach (Histogram.HistogramStep histogramStep in histogram.Steps)
            {
                printTableRows.Add(new[] {
                    $"{histogramStep.RangeHiKey}",
                    $"{histogramStep.RangeRows}",
                    $"{histogramStep.EqRows}",
                    $"{histogramStep.DistinctRangeRows}",
                    $"{histogramStep.AvgRangeRows}"
            });
            }

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobStringIndex(string outputPath, StringIndex stringIndex)
        {
            var printTableTitle = "STRING INDEX";

            var printTableHeaders = new[] {
                "",
                ""
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Left
            };

            var printTableRows = new List<string[]>();

            printTableRows.Add(new[] {
                $"Compressed String",
                $"{stringIndex.CompressedString}"
            });

            printTableRows.Add(new[] {
                $"String Set",
                $"{stringIndex.StringSet}"
            });

            printTableRows.Add(new[] {
                $"Radix Tree",
                $"{stringIndex.RadixTree}"
            });

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobUpdateHistory(string outputPath, UpdateHistory updateHistory)
        {
            var printTableTitle = "UPDATE HISTORY";

            var printTableHeaders = new[] {
                "Updated",
                "Rows",
                "Steps"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Center
            };

            var printTableRows = new List<string[]>();

            foreach (UpdateHistory.UpdateHistoryRecord updateHistoryRecord in updateHistory.updateHistoryRecord)
            {
                printTableRows.Add(new[] {
                    $"{updateHistoryRecord.Updated}",
                    $"{updateHistoryRecord.RowCount}",
                    $"{updateHistoryRecord.StepCount}"
            });
            }

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        public static void PrintStatsBlobSampledScanDump(string outputPath, SampledScanDump sampledScanDump, int vectorCount)
        {
            var printTableTitle = "SAMPLED SCAN DUMP";

            var printTableHeaders = new[] {
                "Vector",
                "Samples"
            };

            var printTableAlignments = new[]
            {
                ASCIITableBuilder.ColumnAlignment.Center,
                ASCIITableBuilder.ColumnAlignment.Left
            };

            var printTableRows = new List<string[]>();

            string currentBucket;

            for (int i = 0; i < vectorCount; i++)
            {
                var vector = sampledScanDump.VectorSamples[i];
                var sb = new StringBuilder();

                for (int j = 0; j < vector.Length; j++)
                {
                    if (j > 0) 
                    { 
                        if (j % 5 == 0) 
                            sb.Append("\n"); 
                        else 
                            sb.Append("  "); 
                    }

                    sb.Append(vector[j]);
                }

                printTableRows.Add(new[]
                {
                    (i + 1).ToString(),
                    sb.ToString()
                });
            }

            var printTable = ASCIITableBuilder.BuildTable(printTableTitle,
                                                          printTableHeaders,
                                                          printTableRows,
                                                          printTableAlignments);

            AppendTable(outputPath, printTable);
        }

        private static string FormatChecksum(uint checksum)
        {
            byte[] bytes = BitConverter.GetBytes(checksum);

            string checksumHex = "0x" + BitConverter.ToString(bytes).Replace("-", "");
            return checksumHex;
        }

        private static void AppendTable(string outputPath, string content)
        {
            File.AppendAllText(outputPath, content + Environment.NewLine + Environment.NewLine);
        }
    }
}

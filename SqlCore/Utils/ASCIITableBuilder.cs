using SqlCore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlCore.Utils.ASCIITableBuilder;

namespace SqlCore.Utils
{
    public static class ASCIITableBuilder
    {
        public enum ColumnAlignment
        {
            Left,
            Center,
            Right
        }

        public static string BuildTable(
            string title,
            string[] headers,
            List<string[]> rows,
            ColumnAlignment[] alignments)
        {
            int columnCount = headers.Length;

            if (alignments.Length != columnCount)
                throw new ArgumentException("Alignments must match column count");

            int[] widths = new int[columnCount];

            for (int i = 0; i < columnCount; i++)
                widths[i] = headers[i].Length;

            foreach (var row in rows)
            {
                for (int i = 0; i < columnCount; i++)
                {
                    string value = row.Length > i ? row[i] ?? "" : "";

                    foreach (var line in value.Split('\n'))
                    {
                        widths[i] = Math.Max(widths[i], line.Length);
                    }
                }
            }

            int totalWidth = widths.Sum(w => w + 2) + widths.Length + 1;

            var sb = new StringBuilder();

            if (!string.IsNullOrWhiteSpace(title))
            {
                sb.AppendLine(BuildFullSeparator(totalWidth, '='));
                sb.AppendLine(BuildTitleRow(title, totalWidth));
                sb.AppendLine(BuildFullSeparator(totalWidth, '='));
            }

            sb.AppendLine(BuildSeparator(widths, '='));
            sb.AppendLine(BuildRow(headers, widths, alignments, isHeader: true));
            sb.AppendLine(BuildSeparator(widths, '='));

            foreach (var row in rows)
            {
                foreach (var line in BuildMultilineRow(row, widths, alignments, false))
                    sb.AppendLine(line);

                sb.AppendLine(BuildSeparator(widths, '-'));
            }

            return sb.ToString();
        }

        private static string BuildSeparator(int[] widths, char separatorChar)
        {
            var sb = new StringBuilder("+");

            foreach (var w in widths)
                sb.Append(new string(separatorChar, w + 2)).Append('+');

            return sb.ToString();
        }

        private static string BuildFullSeparator(int totalWidth, char separatorChar)
        {
            return "+" + new string(separatorChar, totalWidth - 2) + "+";
        }

        private static string BuildTitleRow(string title, int totalWidth)
        {
            int contentWidth = totalWidth - 4;
            string centeredTitle = Center(title, contentWidth);

            return $"| {centeredTitle} |";
        }

        private static string BuildRow(
            string[] values,
            int[] widths,
            ColumnAlignment[] alignments,
            bool isHeader)
        {
            var sb = new StringBuilder("|");

            for (int i = 0; i < widths.Length; i++)
            {
                string value = values.Length > i ? values[i] ?? "" : "";
                string cell = Align(value, widths[i], alignments[i], isHeader);

                sb.Append(' ').Append(cell).Append(' ').Append('|');
            }

            return sb.ToString();
        }

        private static string Align(
            string text,
            int width,
            ColumnAlignment alignment,
            bool isHeader)
        {
            if (isHeader)
                alignment = ColumnAlignment.Center;

            return alignment switch
            {
                ColumnAlignment.Left => text.PadRight(width),
                ColumnAlignment.Right => text.PadLeft(width),
                ColumnAlignment.Center => Center(text, width),
                _ => text
            };
        }

        private static string Center(string text, int width)
        {
            if (text.Length >= width)
                return text;

            int leftPadding = (width - text.Length) / 2;
            int rightPadding = width - text.Length - leftPadding;

            return new string(' ', leftPadding) + text + new string(' ', rightPadding);
        }

        private static IEnumerable<string> BuildMultilineRow(
            string[] values,
            int[] widths,
            ColumnAlignment[] alignments,
            bool isHeader)
        {
            var splitValues = values
                .Select((v, i) => (Lines: (v ?? "").Split('\n'), Index: i))
                .ToArray();

            int maxLines = splitValues.Max(x => x.Lines.Length);

            for (int line = 0; line < maxLines; line++)
            {
                var row = new string[values.Length];

                for (int col = 0; col < values.Length; col++)
                {
                    row[col] = line < splitValues[col].Lines.Length
                        ? splitValues[col].Lines[line]
                        : "";
                }

                yield return BuildRow(row, widths, alignments, isHeader && line == 0);
            }
        }
    }
}







//var headers = new[]
//{
//    "Col1",
//    "Col2",
//    "Col3",
//    "Numeric Column"
//};

//var rows = new List<string[]>
//{
//    new[] { "Value 1", "Value 2", "123", "10.0" },
//    new[] { "Separate", "cols", "with a tab or 4 spaces", "-2,027.1" },
//    new[] { "This is a row with only one cell" }
//};

//var alignments = new[]
//{
//    ColumnAlignment.Left,
//    ColumnAlignment.Left,
//    ColumnAlignment.Left,
//    ColumnAlignment.Right
//};

//string table = AsciiTableBuilder.BuildTable(headers, rows, alignments);
//File.WriteAllText("tabela.txt", table);
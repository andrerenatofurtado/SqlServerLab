using System.Text;

namespace SqlCore.Engine.Statistics.StatsBlob
{
    public class StringIndex
    {
        private int compressedStringSize;

        public string CompressedString;
        public string StringSet;
        public string RadixTree;

        public StringIndex(byte[] stringIndexData, bool isUnicode)
        {
            compressedStringSize = BitConverter.ToInt32(stringIndexData, 33);
            if (isUnicode)
                CompressedString = Encoding.Unicode.GetString(stringIndexData, 37, compressedStringSize);
            else
                CompressedString = Encoding.ASCII.GetString(stringIndexData, 37, compressedStringSize);

            var nodeDescriptorsCount = BitConverter.ToInt32(stringIndexData, compressedStringSize + 37);

            var nodeDescriptorsDataSize = (nodeDescriptorsCount * 5);

            byte[] nodeDescriptorsData = new byte[nodeDescriptorsDataSize];
            Array.Copy(stringIndexData, compressedStringSize + 37 + 4, nodeDescriptorsData, 0, nodeDescriptorsDataSize);

            var tree = ParseTree(nodeDescriptorsData, CompressedString);

            StringSet = string.Join(",", ExtractStringSet(nodeDescriptorsData, CompressedString));

            RadixTree = PrintTree(tree);

        }

        public class RadixNode
        {
            public string Text { get; set; }
            public bool EndWord { get; set; }
            public List<RadixNode> Children { get; } = new();

            public RadixNode(string text, bool endWord)
            {
                Text = text;
                EndWord = EndWord;
            }
        }

        public static IEnumerable<string> ExtractStringSet(byte[] nodeDescriptorsData, string compressedString)
        {
            Stack<string> levels = new();
            string prefix = "";

            for (int i = 5; i < nodeDescriptorsData.Length; i += 5)
            {
                byte flags = nodeDescriptorsData[i];
                byte length = nodeDescriptorsData[i + 1];
                int offset = (nodeDescriptorsData[i + 2]) 
                                | (nodeDescriptorsData[i + 3] << 8) 
                                    | (nodeDescriptorsData[i + 4] << 16);

                string text = compressedString.Substring(offset, length);

                bool closeString = (flags & 0x01) != 0;
                bool pushLevel = (flags & 0xC0) == 0xC0;
                bool popLevel = flags == 0x01;

                string str = prefix + text;

                if (closeString)
                    yield return str;

                if (pushLevel)
                {
                    levels.Push(text);
                    prefix += text;
                }
                else if (flags == 0x40)
                {
                    prefix += text;
                }
                else if (flags == 0x81)
                {

                }
                else if (popLevel)
                {
                    if (levels.Count > 0)
                    {
                        string removed = levels.Pop();
                        prefix = prefix.Substring(0, prefix.Length - removed.Length);
                    }

                    if (levels.Count == 0)
                        prefix = "";
                }
            }
        }

        public static RadixNode ParseTree(byte[] nodeDescriptorsData, string compressedString)
        {
            var root = new RadixNode("", false);

            Stack<RadixNode> stack = new();
            stack.Push(root);

            RadixNode current = root;

            for (int i = 5; i < nodeDescriptorsData.Length; i += 5)
            {
                byte flags = nodeDescriptorsData[i];
                byte length = nodeDescriptorsData[i + 1];
                int offset = (nodeDescriptorsData[i + 2]) 
                                | (nodeDescriptorsData[i + 3] << 8) 
                                    | (nodeDescriptorsData[i + 4] << 16);

                string text = compressedString.Substring(offset, length);

                bool endWord = (flags & 0x01) != 0;

                bool push = (flags & 0xC0) == 0xC0;
                bool noLevel = flags == 0x40;
                bool keepLevel = flags == 0x81;
                bool pop = flags == 0x01;

                var node = new RadixNode(text, endWord);

                current.Children.Add(node);

                if (push)
                {
                    stack.Push(node);
                    current = node;
                }
                else if (noLevel)
                {
                    current = node;
                }
                else if (keepLevel)
                {
                    
                }
                else if (pop)
                {
                    if (stack.Count > 1)
                        stack.Pop();

                    current = stack.Peek();
                }
            }

            return root;
        }

        public static string PrintTree(RadixNode root)
        {
            var sb = new StringBuilder();

            for (int i = 0; i < root.Children.Count; i++)
            {
                bool last = i == root.Children.Count - 1;
                PrintNode(root.Children[i], sb, "", last);
            }

            return sb.ToString();
        }

        private static void PrintNode(RadixNode node, StringBuilder sb, string prefix, bool isLast)
        {
            sb.Append(prefix);
            sb.Append(isLast ? "└── " : "├── ");
            sb.Append(node.Text);

            if (node.EndWord)
                sb.Append(" *");

            sb.Append("\n");

            prefix += isLast ? "    " : "│   ";

            for (int i = 0; i < node.Children.Count; i++)
            {
                bool last = i == node.Children.Count - 1;
                PrintNode(node.Children[i], sb, prefix, last);
            }
        }

    }
}

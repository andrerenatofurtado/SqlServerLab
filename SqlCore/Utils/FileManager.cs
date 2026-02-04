using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCore.Utils
{
    public static class FileManager
    {
        public static byte[] ReadFileBytes(string filePath, long offset, int size)
        {
            using var fs = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read);

            fs.Seek(offset, SeekOrigin.Begin);

            byte[] buffer = new byte[size];
            int bytesRead = fs.Read(buffer, 0, size);

            return buffer;
        }

        public static long GetFileSizeBytes(string filePath)
        {
            return new FileInfo(filePath).Length;
        }
    }
}

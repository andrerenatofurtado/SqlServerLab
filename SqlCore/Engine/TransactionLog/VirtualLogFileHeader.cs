using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCore.Engine.TransactionLog
{
    public class VirtualLogFileHeader
    {
        public byte parity;
        public byte version;
        public int fSeqNo;
        public int writeSeqNo;
        public long fileSize;
        public long startOffset;
        public string createLsn;
    }
}

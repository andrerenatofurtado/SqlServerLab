using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCore.Engine.TransactionLog
{
    public class LogBlockHeader
    {
        public byte sectorFlags;

        public byte version;
        public byte parity;

        public ushort numOfRecords;
        public ushort offsetSlotArray;

        public ushort blkSize;

        public byte flags;

        public ushort prevBlkSize;

        public int fSeqNo;

        public uint checksum;

        public DateTime closeTime;

        public bool firstSector;
        public bool lastSector;

        public bool hasChecksum;
        public bool isTDEEncrypted;

    }
}

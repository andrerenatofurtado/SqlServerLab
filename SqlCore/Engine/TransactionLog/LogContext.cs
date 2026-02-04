using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCore.Engine.TransactionLog
{
    public static class LogContext
    {
        public static string GetLogContextDesc(byte logContext) =>
            logContext switch
            {
                0x00 => "LCX_NULL",
                0x01 => "LCX_HEAP",
                0x02 => "LCX_CLUSTERED",
                0x03 => "LCX_INDEX_LEAF",
                0x04 => "LCX_INDEX_INTERIOR",
                0x05 => "LCX_TEXT_MIX",
                0x06 => "LCX_TEXT_TREE",
                0x07 => "LCX_DIAGNOSTICS",
                0x08 => "LCX_GAM",
                0x09 => "LCX_SGAM",
                0x0a => "LCX_IAM",
                0x0b => "LCX_PFS",
                0x0c => "LCX_IDENTITY_VALUE",
                0x0d => "LCX_OBJECT_ID",
                0x0e => "LCX_NONSYS_SPLIT",
                0x11 => "LCX_FILE_HEADER",
                0x12 => "LCX_SCHEMA_VERSION",
                0x13 => "LCX_MARK_AS_GHOST",
                0x14 => "LCX_BOOT_PAGE",
                0x15 => "LCX_SYSCONFIG_PAGE",
                0x16 => "LCX_CTR_ABORTED",
                0x17 => "LCX_BOOT_PAGE_CKPT",
                0x18 => "LCX_DIFF_MAP",
                0x19 => "LCX_ML_MAP",
                0x1a => "LCX_REMOVE_VERSION_INFO",
                0x1b => "LCX_DBCC_FORMATTED",
                0x1c => "LCX_UNLINKED_REORG_PAGE",
                0x1d => "LCX_BULK_OPERATION_PAGE",
                0x1e => "LCX_TRACKED_XDES",
                0x1f => "LCX_ENCRYPT_UNALLOC_PAGE",
                0x20 => "LCX_SORT_PAGE",
                0x21 => "LCX_WORK_FILE_PAGE",
                0x22 => "LCX_RESTORE_BAD_UNALLOC_PAGE",
                0x23 => "LCX_OFF_ROW_PVS",
                0x24 => "LCX_CTR_XACT_CLEANUP",
                0x25 => "LCX_TRY_TRANSITION_TO_NON_CTR",
                0x26 => "LCX_CTR_NEST_ABORTED",
                0x27 => "LCX_CTR_LOG_HOLDUP",
                0x28 => "LCX_LEDGER_TRANSACTIONS"
            };
    }
}

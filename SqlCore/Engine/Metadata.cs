namespace SqlCore.Engine
{
    public static class Metadata
    {
        private static int seed = 0xF42439;

        public static int GenerateNextMetadataObjectId(int objid)
        {
            long result = (long)objid + seed;

            if (result <= int.MaxValue)
                return (int)result;

            return (int)(result - int.MaxValue - 1);
        }
    }
}

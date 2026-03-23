namespace SqlCore.Engine.SqlTypes
{
    public static class SqlFloat
    {
        public static object Parse(byte[] data)
        {
            return data.Length == 4 ? BitConverter.ToSingle(data, 0) : BitConverter.ToDouble(data, 0);
        }
    }
}

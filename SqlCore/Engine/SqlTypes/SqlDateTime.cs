using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCore.Engine.SqlTypes
{
    public static class SqlDateTime
    {
        private const double CLOCK_TICK_MS = 10d / 3d;

        public static DateTime Parse(long data)
        {
            int date = (int)(data >> 32);
            int time = (int)(data & 0xFFFFFFFF);

            return new DateTime(1900, 1, 1).AddMilliseconds(time * CLOCK_TICK_MS).AddDays(date);
        }
    }
}

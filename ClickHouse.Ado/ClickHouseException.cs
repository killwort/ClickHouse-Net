using System;

namespace ClickHouse.Ado
{
    public class ClickHouseException : Exception
    {
        public ClickHouseException()
        {
        }

        public ClickHouseException(string message) : base(message)
        {
        }

        public ClickHouseException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public string Name;
        public int Code;
        public string ServerStackTrace;
    }
}
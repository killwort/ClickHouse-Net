using System;

namespace ClickHouse.Ado {
    public class ClickHouseException : Exception {
        public int Code;

        public string Name;
        public string ServerStackTrace;

        public ClickHouseException() { }

        public ClickHouseException(string message) : base(message) { }

        public ClickHouseException(string message, Exception innerException) : base(message, innerException) { }
    }
}
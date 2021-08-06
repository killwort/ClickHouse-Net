using ClickHouse.Ado;
using System;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var connectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=clickhouse;Port=9000;User=default;Password=;SocketTimeout=600000;Database=Test;SslEnabled=True";
            var expectedSettings = new ClickHouseConnectionSettings(connectionString);
        }
    }
}

using System;
using ADBRO.EventHubClickHouseConsumer;
using ClickHouse.Ado;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {

            var a1 = new EventEntity();
            var a2 = new EventEntity();

            ClickHouseClient.InsertRange(new EventEntity[] { a1, a2 }, EventEntity.BULK_INSERT_SQL);

            Console.WriteLine("Hello World!");
        }
    }
}

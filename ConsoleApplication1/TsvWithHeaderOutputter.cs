using System;
using System.IO;

namespace ClickHouse.Isql
{
    class TsvWithHeaderOutputter : TsvOutputter
    {
        public override void HeaderCell(string name)
        {
            Console.Write(name);
            Console.Write('\t');
        }

        public override void DataStart()
        {
            Console.WriteLine("\n--------------------------------------------------------");
        }

        public TsvWithHeaderOutputter(Stream s) : base(s)
        {
        }
    }
}
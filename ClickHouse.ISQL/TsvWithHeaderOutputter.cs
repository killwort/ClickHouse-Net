using System;
using System.IO;

namespace ClickHouse.Isql {
    internal class TsvWithHeaderOutputter : TsvOutputter {
        public TsvWithHeaderOutputter(Stream s) : base(s) { }

        public override void HeaderCell(string name) {
            Console.Write(name);
            Console.Write('\t');
        }

        public override void DataStart() => Console.WriteLine("\n--------------------------------------------------------");
    }
}
using System;
using System.IO;
using System.Text;

namespace ClickHouse.Isql {
    internal class TsvOutputter : Outputter {
        private TextWriter writer;

        public TsvOutputter(Stream s) => writer = new StreamWriter(s, Encoding.UTF8);

        public override void ResultStart() { }

        public override void ResultEnd() { }

        public override void RowStart() { }

        public override void RowEnd() => Console.WriteLine();

        public override void HeaderCell(string name) { }

        public override void ValueCell(object value) {
            Console.Write(value);
            Console.Write('\t');
        }

        public override void DataStart() { }
    }
}
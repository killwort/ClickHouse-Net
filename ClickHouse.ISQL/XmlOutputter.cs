using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;

namespace ClickHouse.Isql {
    internal class XmlOutputter : Outputter {
        private readonly XmlTextWriter writer;
        private int cell;
        private List<string> names;

        public XmlOutputter(Stream s) => writer = new XmlTextWriter(s, Encoding.UTF8);

        public override void Start() {
            writer.WriteStartDocument(true);
            writer.WriteStartElement("Results");
        }

        public override void End() {
            writer.WriteEndDocument();
            writer.Flush();
        }

        public override void ResultStart() {
            writer.WriteStartElement("Result");
            names = new List<string>();
        }

        public override void ResultEnd() => writer.WriteEndElement();

        public override void RowStart() {
            cell = 0;
            writer.WriteStartElement("Row");
        }

        public override void RowEnd() => writer.WriteEndElement();

        public override void HeaderCell(string name) => names.Add(name);

        public override void ValueCell(object value) {
            writer.WriteStartElement(names[cell++]);
            writer.WriteValue(value?.ToString() ?? "");
            writer.WriteEndElement();
        }

        public override void DataStart() { }
    }
}
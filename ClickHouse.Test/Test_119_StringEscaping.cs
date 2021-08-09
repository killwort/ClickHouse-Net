using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_119_StringEscaping {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_119_stringescaping").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_119_stringescaping (a String) ENGINE = Memory").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestParse(
            [Values(
                "INSERT INTO \"StoredClass\" ( \"primaryKey\", \"StoredProperty\" ) VALUES ('7a25da2e-e1bc-4d73-8068-ec4cc507c649','abc')",
                "insert into test_119_stringescaping (a) values ('CSA_C[{\\\"field\\\":\\\"Field11\\\"}]PT\\\"Y1233',0)",
                "INSERT INTO `StoredClass` ( `primaryKey`, `StoredProperty` ) VALUES ('7a25da2e-e1bc-4d73-8068-ec4cc507c649','[{\\\"field\\\":\\\"Field11\\\"}]')",
                "INSERT INTO \"StoredClass\" ( \"primaryKey\", \"StoredProperty\" ) VALUES ('7a25da2e-e1bc-4d73-8068-ec4cc507c649','[{\\\"field\\\":\\\"Field11\\\"}]')"
                )]string sql)
        {
            var insertParser = new ClickHouse.Ado.Impl.ATG.Insert.Parser(new ClickHouse.Ado.Impl.ATG.Insert.Scanner(new MemoryStream(Encoding.UTF8.GetBytes(sql))));
            insertParser.errors.errorStream = new StringWriter();
            insertParser.Parse();
            Assert.Zero(insertParser.errors.count);
        }
        [Test]
        public void TestInsert() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                var result = cnn.CreateCommand("insert into test_119_stringescaping (a) values ('CSA_C[{\\\"field\\\":\\\"Field11\\\"}]PT\\\"Y1233',0)").ExecuteNonQuery();

            }
        }
    }
}

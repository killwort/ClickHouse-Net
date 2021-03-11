using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_87_LowCardinality {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_87_lowcardinality").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_87_lowcardinality (a LowCardinality(String)) ENGINE = Memory").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void Test() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                var items = new List<string>();
                for (var i = 0; i < 1000; i++)
                    items.Add(((char) ('A' + (i % 20))).ToString());
                var result = cnn.CreateCommand("INSERT INTO test_87_lowcardinality (a) VALUES @bulk").AddParameter("bulk", DbType.Object, items.Select(x => (object) new object[] {x}).ToArray())
                                .ExecuteNonQuery();

                var values = new List<string>();
                using (var cmd = cnn.CreateCommand("SELECT a FROM test_87_lowcardinality"))
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(r.GetString(0)); });
                }
                Assert.IsTrue(items.SequenceEqual(values));
            }
        }
    }
}

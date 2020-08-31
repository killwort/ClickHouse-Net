using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_100_BulkArrayFirst {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_100_arr_first").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_100_arr_first (d Date, a Array(String) ) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestRoundtrip() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_100_arr_first (a,d) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {
                       new object[] {new string[]{"a","b"}, DateTime.Now}
                   })
                   .ExecuteNonQuery();
                var values = new List<Tuple<DateTime, string[]>>();
                using (var cmd = cnn.CreateCommand("SELECT d,a FROM test_100_arr_first ORDER BY d"))
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(Tuple.Create(r.GetDateTime(0), ((object[])r.GetValue(1)).OfType<string>().ToArray())); });
                }


                Assert.Greater(values.Count,0);

                Assert.AreEqual("a", values[0].Item2[0]);
                Assert.AreEqual("b", values[0].Item2[1]);
            }
        }
    }
}

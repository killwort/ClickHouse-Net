using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_84_DecimalSupport {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_decimal").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_decimal (k Date, d Decimal(20,4))  ENGINE = MergeTree(k, (d), 8192)").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestRoudtrip() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_decimal (k, d) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {new object[] {DateTime.Now, 387m}, new object[] {DateTime.Now, 0m}})
                   .ExecuteNonQuery();
                var values = new List<Tuple<DateTime, decimal>>();
                using (var cmd = cnn.CreateCommand("SELECT k, d FROM test_decimal ORDER BY d"))
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(Tuple.Create(r.GetDateTime(0), r.GetDecimal(1))); });
                }

                Assert.AreEqual(0,(double) values[0].Item2, .33);
                Assert.AreEqual(387,(double) values[1].Item2, .33);
            }
        }
    }
}
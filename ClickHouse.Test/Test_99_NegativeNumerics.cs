using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_99_NegativeNumerics {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_99_neg_num").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_99_neg_num (FixedDate Date, Amount1 Decimal(16,4), Amount2 Decimal(20,8), Amount3 Decimal(22,10) ) ENGINE = MergeTree PARTITION BY toYYYYMM(FixedDate) ORDER BY FixedDate SETTINGS index_granularity = 8192").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestRoundtrip([Values(-100, -40.96, 40.96, 100, -1234567890.1234, 1234567890.1234)]decimal value) {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_99_neg_num (FixedDate,Amount1,Amount2,Amount3) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {
                       new object[] {DateTime.Now, value,value,value}
                   })
                   .ExecuteNonQuery();
                var values = new List<Tuple<DateTime, decimal,decimal,decimal>>();
                using (var cmd = cnn.CreateCommand("SELECT FixedDate,Amount1,Amount2,Amount3 FROM test_99_neg_num ORDER BY FixedDate"))
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(Tuple.Create(r.GetDateTime(0), r.GetDecimal(1),r.GetDecimal(2),r.GetDecimal(3))); });
                }

                var row = values.FirstOrDefault(x => decimal.Equals(x.Item2, value));
                Assert.IsNotNull(row);
                Assert.AreEqual(value, row.Item2);
                Assert.AreEqual(value, row.Item3);
                Assert.AreEqual(value, row.Item4);
            }
        }
    }
}

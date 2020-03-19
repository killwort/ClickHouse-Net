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
                cnn.CreateCommand("CREATE TABLE test_decimal (k Date, d Decimal(27,10))  ENGINE = MergeTree(k, (d), 8192)").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestInsertBulk() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_decimal (k, d) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {new object[] {DateTime.Now, 387m}, new object[] {DateTime.Now, 0m}})
                   .ExecuteNonQuery();
            }
        }
    }
}
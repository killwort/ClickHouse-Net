using System;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class TestDateTime64Support {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_dt64").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_dt64 (k Date, dt64 DateTime64(5), dt64tz DateTime64(5,'Europe/Moscow'))  ENGINE = MergeTree(k, (dt64,dt64tz), 8192)").ExecuteNonQuery();
            }
            Thread.Sleep(1000);
        }

        [Test]
        public void TestInsertLiteral() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_dt64 (k, dt64, dt64tz) VALUES ('2020-01-01','2020-01-01 00:00:00','2020-01-01 00:00:00')").ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertLiteralParameter() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_dt64 (k, dt64, dt64tz) VALUES ('2020-01-01',@p1,@p2)").AddParameter("p1", DbType.DateTime, DateTime.Now)
                   .AddParameter("p2", DbType.DateTime, DateTime.Now).ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertBulk() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_dt64 (k, dt64, dt64tz) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {new object[] {DateTime.Now, DateTime.Now, DateTime.Now}})
                   .ExecuteNonQuery();
            }
        }
    }
}
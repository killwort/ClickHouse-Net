using System;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_82_NullableFixedString {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_nfs").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_nfs (k Date, a Int32, nfs Nullable(FixedString(2)))  ENGINE = MergeTree(k, (a), 8192)").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestInsertBulk() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_nfs (k, nfs) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {new object[] {DateTime.Now, "aa"}, new object[] {DateTime.Now, null}})
                   .ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertLiteral() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_nfs (k, nfs) VALUES ('2020-01-01',null)").ExecuteNonQuery();
            }
        }
    }
}
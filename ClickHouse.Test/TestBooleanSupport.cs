using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class TestBooleanSupport {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_bool").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_bool (bool_column Bool)  ENGINE = MergeTree ORDER BY bool_column SETTINGS index_granularity = 8192").ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertBulk() {
            var value = true;
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_bool (bool_column) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { true }, new object[] { false } }).ExecuteNonQuery();
            }

            var values = SelectValues();
            Assert.True(values[0] ^ values[1]);
        }

        [Test]
        public void TestInsertLiteral() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand($"INSERT INTO test_bool (bool_column) VALUES (true),(false)").ExecuteNonQuery();
            }

            var values = SelectValues();
            Assert.True(values[0] ^ values[1]);
        }

        [Test]
        public void TestInsertLiteralParameter() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO test_bool (bool_column) VALUES (@p1)").AddParameter("p1", DbType.Boolean, true).ExecuteNonQuery();
            }

            var values = SelectValues();
            Assert.True(values[0]);
        }

        private List<bool> SelectValues() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                var values = new List<bool>();
                using (var cmd = cnn.CreateCommand("SELECT bool_column FROM test_bool"))
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(r.GetBoolean(0)); });
                }

                return values;
            }
        }
    }
}

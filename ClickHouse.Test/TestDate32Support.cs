using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test
{
    [TestFixture]
    public class TestDate32Support
    {
        [OneTimeSetUp]
        public void CreateStructures()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_date32").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_date32 (date32_column Date32)  ENGINE = MergeTree ORDER BY date32_column SETTINGS index_granularity = 8192").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        [Test]
        public void TestInsertBulk()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { DateTime.Now } })
                   .ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertLiteral()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES ('2020-01-01')").ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertLiteralParameter()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES (@p1)").AddParameter("p1", DbType.DateTime, DateTime.Now).ExecuteNonQuery();
            }
        }

        [Test]
        public void TestSelect()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES (@p)")
                   .AddParameter("p", DbType.DateTime, new DateTime(2000, 01, 02))
                   .ExecuteNonQuery();
                var values = new List<DateTime>();
                using (var cmd = cnn.CreateCommand("SELECT date32_column FROM test_date32"))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.ReadAll(r => { values.Add(r.GetDateTime(0)); });
                }
            }
        }
    }
}
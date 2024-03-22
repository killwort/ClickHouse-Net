using System.Collections.Generic;
using System.Data;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test
{

    [TestFixture]
    public class TestInsertWithSettings
    {
        [OneTimeSetUp]
        public void CreateStructures()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_bool").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_bool (k Int32,bool_column Bool)  ENGINE = MergeTree ORDER BY (k,bool_column) SETTINGS index_granularity = 8192").ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertBulk()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_bool (k, bool_column) SETTINGS async_insert= 1, wait_for_async_insert =1 VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { 1, true }, new object[] { 1, false } })
                    .ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertLiteral()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("INSERT INTO test_bool (k,bool_column) settings async_insert=1, wait_for_async_insert=1 VALUES (2,true),(2,false)").ExecuteNonQuery();
            }
        }
    }
}

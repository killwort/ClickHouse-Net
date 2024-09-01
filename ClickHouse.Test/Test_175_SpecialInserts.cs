using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Test;

public class Test_175_SpecialInserts
{
    [OneTimeSetUp]
    public void CreateStructures()
    {
        using (var cnn = ConnectionHandler.GetConnection())
        {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_175_special_inserts").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_175_special_inserts (id Int32) ENGINE = MergeTree ORDER BY id").ExecuteNonQuery();
        }

        Thread.Sleep(1000);
    }

    [Test]
    public async Task TestInsertNoParameters()
    {
        using (var cnn = ConnectionHandler.GetConnection())
        {
            cnn.CreateCommand("INSERT INTO test_175_special_inserts SELECT * FROM numbers(100)").ExecuteNonQuery();
        }
    }


    [Test]
    public async Task TestInsertIntoFunctionNoParameters()
    {
        using (var cnn = ConnectionHandler.GetConnection())
        {
            var insertCount = cnn.CreateCommand("INSERT INTO TABLE FUNCTION remoteSecure('localhost',default.test_175_special_inserts) SELECT * FROM numbers(100)").ExecuteNonQuery();
            Assert.AreEqual(insertCount, 0); //Clickhouse does not output "number of rows affected".
            var records = cnn.CreateCommand("SELECT count() from test_175_special_inserts").ExecuteScalar();
            Assert.AreEqual(100, records);
        }
    }
}

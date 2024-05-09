using System;
using System.Data;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

public class Test_166_JSONColumns
{
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_166_json").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_166_json (session_id String, guess JSON, timestamp DateTime DEFAULT now() CODEC(Delta(4), ZSTD(1)) ) ENGINE = MergeTree ORDER BY session_id").ExecuteNonQuery();
        }

        Thread.Sleep(1000);
    }

    [Test]
    public async Task TestInsertBulk()
    {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_166_json (session_id, guess, timestamp) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[]
            {
                new object[] { "1", "{\"name\":\"value\"}", DateTime.UtcNow }
            }).ExecuteNonQuery();
        }

        var values = SelectValue("1");
        Assert.True(values.Equals("{\"name\":\"value\"}"));
    }
    
    private string SelectValue(string k) {
        using (var cnn = ConnectionHandler.GetConnection())
        {
            string rv = null;
            using (var cmd = cnn.CreateCommand("SELECT guess FROM test_166_json WHERE session_id=@k")) {
                cmd.AddParameter("k", k);
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(
                        r =>
                        {
                            rv = r.GetString(1);
                        }
                    );
                }
            }

            return rv;
        }
    }

}

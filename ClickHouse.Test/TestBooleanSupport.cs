using System.Collections.Generic;
using System.Data;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class TestBooleanSupport {
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_bool").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_bool (k Int32,bool_column Bool)  ENGINE = MergeTree ORDER BY (k,bool_column) SETTINGS index_granularity = 8192").ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertBulk() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_bool (k, bool_column) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { 1, true }, new object[] { 1, false } }).ExecuteNonQuery();
        }

        var values = SelectValues(1);
        Assert.True(values[0] ^ values[1]);
    }

    [Test]
    public void TestInsertLiteral() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_bool (k,bool_column) VALUES (2,true),(2,false)").ExecuteNonQuery();
        }

        var values = SelectValues(2);
        Assert.True(values[0] ^ values[1]);
    }

    [Test]
    public void TestInsertLiteralParameter() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_bool (k,bool_column) VALUES (3,@p1)").AddParameter("p1", DbType.Boolean, true).ExecuteNonQuery();
        }

        var values = SelectValues(3);
        Assert.True(values[0]);
    }

    private List<bool> SelectValues(int k) {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var values = new List<bool>();
            using (var cmd = cnn.CreateCommand("SELECT bool_column FROM test_bool WHERE k=@k")) {
                cmd.AddParameter("k", k);
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(r => { values.Add(r.GetBoolean(0)); });
                }
            }

            return values;
        }
    }
}

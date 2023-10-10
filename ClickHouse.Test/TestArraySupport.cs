using System.Collections.Generic;
using System.Data;
using System.Linq;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class TestArraySupport {
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_array").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_array (ids Array(UInt64))  ENGINE = MergeTree ORDER BY ids SETTINGS index_granularity = 8192").ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertEmptyArray() {
        var value = Enumerable.Empty<ulong>().ToArray();
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_array (ids) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { value } }).ExecuteNonQuery();
        }

        var values = SelectValues();
        Assert.AreEqual(value, values[0]);
    }

    private List<ulong[]> SelectValues() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var values = new List<ulong[]>();
            using (var cmd = cnn.CreateCommand("SELECT ids FROM test_array"))
            using (var reader = cmd.ExecuteReader()) {
                reader.ReadAll(r => { values.Add((ulong[])r.GetValue(0)); });
            }

            return values;
        }
    }
}
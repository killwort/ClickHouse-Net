using System;
using System.Collections.Generic;
using System.Data;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class TestDate32Support {
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_date32").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_date32 (date32_column Date32)  ENGINE = MergeTree ORDER BY date32_column SETTINGS index_granularity = 8192").ExecuteNonQuery();

            cnn.CreateCommand("DROP TABLE IF EXISTS test_date32_array").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_date32_array (date32_column Array(Date32))  ENGINE = MergeTree ORDER BY date32_column SETTINGS index_granularity = 8192").ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertBulk() {
        var value = new DateTime(2000, 01, 02);
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { value } }).ExecuteNonQuery();
        }

        var values = SelectValues();
        Assert.AreEqual(value, values[0]);
    }

    [Test]
    public void TestInsertBulkArray() {
        var value = new[] { new DateTime(2000, 01, 02) };
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_date32_array (date32_column) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { value } }).ExecuteNonQuery();
        }

        var values = SelectArrayValues();
        Assert.AreEqual(value, values[0]);
    }

    [Test]
    public void TestInsertLiteral() {
        var value = new DateTime(2000, 01, 02);
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand($"INSERT INTO test_date32 (date32_column) VALUES ('{value.ToString("yyyy-MM-dd")}')").ExecuteNonQuery();
        }

        var values = SelectValues();
        Assert.AreEqual(value, values[0]);
    }

    [Test]
    public void TestInsertLiteralParameter() {
        var value = new DateTime(2000, 01, 02);
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_date32 (date32_column) VALUES (@p1)").AddParameter("p1", DbType.DateTime, value).ExecuteNonQuery();
        }

        var values = SelectValues();
        Assert.AreEqual(value, values[0]);
    }

    private List<DateTime> SelectValues() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var values = new List<DateTime>();
            using (var cmd = cnn.CreateCommand("SELECT date32_column FROM test_date32"))
            using (var reader = cmd.ExecuteReader()) {
                reader.ReadAll(r => { values.Add(r.GetDateTime(0)); });
            }

            return values;
        }
    }

    private List<DateTime[]> SelectArrayValues() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var values = new List<DateTime[]>();
            using (var cmd = cnn.CreateCommand("SELECT date32_column FROM test_date32_array"))
            using (var reader = cmd.ExecuteReader()) {
                reader.ReadAll(r => { values.Add((DateTime[])r.GetValue(0)); });
            }

            return values;
        }
    }
}
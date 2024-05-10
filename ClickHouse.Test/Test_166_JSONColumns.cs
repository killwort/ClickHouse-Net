using System;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

public class Test_166_JSONColumns
{
    [OneTimeSetUp]
    public void CreateDatabase()
    {
        using var connection = ConnectionHandler.GetConnection();
        connection.CreateCommand("SET allow_experimental_object_type = 1").ExecuteNonQuery();
        connection.CreateCommand("DROP TABLE IF EXISTS json_test").ExecuteNonQuery();
        connection.CreateCommand("""
                                 CREATE TABLE json_test (
                                     json Object('json'),
                                     timestamp DateTime
                                 )
                                    ENGINE = MergeTree()
                                    PARTITION BY toYYYYMM(timestamp)
                                    ORDER BY (timestamp)
                                 """).ExecuteNonQuery();

        Thread.Sleep(1000);
    }

    [Test]
    public void TestSimple()
    {
        // Arrange
        using var connection = ConnectionHandler.GetConnection();
        var command = connection.CreateCommand("INSERT INTO json_test (json, timestamp) VALUES (@json, @timestamp)")
            .AddParameter("json", """{"a": 1, "b": 2}""")
            .AddParameter("timestamp", DbType.DateTime, DateTime.Now);

        // Act & Assert
        Assert.DoesNotThrow(() => command.ExecuteNonQuery());
    }
}

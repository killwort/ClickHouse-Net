using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class TestMapSupport
{
    [OneTimeSetUp]
    public void CreateDatabase()
    {
        using var connection = ConnectionHandler.GetConnection();

        connection.CreateCommand("DROP TABLE IF EXISTS map_test").ExecuteNonQuery();
        connection.CreateCommand(
            """
            CREATE TABLE map_test (
                map Map(String, String),
                timestamp DateTime
            )
            ENGINE=MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (timestamp, map)
            """
        ).ExecuteNonQuery();

        Thread.Sleep(1000);
    }

    private class TestEntity : IEnumerable
    {
        public TestEntity(IDataRecord reader)
        {
            Map = reader.GetValue(reader.GetOrdinal("map")) as Dictionary<string, string>;
            Timestamp = reader.GetDateTime(reader.GetOrdinal("timestamp"));
        }

        public TestEntity(Dictionary<string, string> map, DateTime timestamp)
        {
            Map = map;
            Timestamp = timestamp;
        }

        public Dictionary<string, string> Map { get; set; }

        public DateTime Timestamp { get; set; }

        public IEnumerator GetEnumerator()
        {
            yield return Map;
            yield return Timestamp;
        }
    }

    [Test]
    public void TestSelect()
    {
        // Arrange
        var entities = new List<TestEntity>
        {
            new(new Dictionary<string, string> { { "key1", "value1" }, { "key2", "value2" } }, DateTime.Now),
            new(new Dictionary<string, string> { { "key3", "value3" }, { "key4", "value4" } }, DateTime.Now),
            new(new Dictionary<string, string> { { "key5", "value5" }, { "key6", "value6" } }, DateTime.Now),
            new(new Dictionary<string, string> { { "key7", "value7" }, { "key8", "value8" } }, DateTime.Now)
        };

        using var connection = ConnectionHandler.GetConnection();

        var cmd = connection.CreateCommand("INSERT INTO map_test (map, timestamp) VALUES @bulk");
        cmd.AddParameter("bulk", DbType.Object, entities);
        cmd.ExecuteNonQuery();
        
        // Act
        var result = new List<TestEntity>();
        using var reader = connection.CreateCommand("SELECT map, timestamp FROM map_test").ExecuteReader();
        do
        {
            while (reader.Read())
            {
                result.Add(new TestEntity(reader));
            }
        } while (reader.NextResult());
        
        // Assert
        Assert.AreEqual(entities.Count, result.Count);
        for (var i = 0; i < entities.Count; i++)
        {
            Assert.AreEqual(entities[i].Map.Count, result[i].Map.Count);
            foreach (var key in entities[i].Map.Keys)
            {
                Assert.IsTrue(result[i].Map.ContainsKey(key));
                Assert.AreEqual(entities[i].Map[key], result[i].Map[key]);
            }
        }
    }

    [Test]
    public void TestSingle()
    {
        // Arrange
        var map = new Dictionary<string, string> { { "key1", "value1" }, { "key2", "value2" } };
        var timestamp = DateTime.Now;
        
        using var connection = ConnectionHandler.GetConnection();
        
        var cmd = connection.CreateCommand("INSERT INTO map_test (map, timestamp) VALUES (@map, @timestamp)");
        cmd.AddParameter("map", map);
        cmd.AddParameter("timestamp", DbType.DateTime, timestamp);
        cmd.ExecuteNonQuery();
        
        // Act
        using var reader = connection.CreateCommand("SELECT map, timestamp FROM map_test").ExecuteReader();
        reader.NextResult();
        reader.Read();
        var result = new TestEntity(reader);
        
        // Assert
        Assert.AreEqual(map.Count, result.Map.Count);
    }
}
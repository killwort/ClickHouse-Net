using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class TestGuidSupport {
        [OneTimeSetUp]
        public void CreateDatabase() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS guid_test").ExecuteNonQuery();
                cnn.CreateCommand(
                    @"  CREATE TABLE guid_test (
                        guid UUID, 
                        dt DateTime)  
                        ENGINE=MergeTree()
                        PARTITION BY toYYYYMM(dt)
                        ORDER BY (dt, guid)"
                ).ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }

        public class TestEntity : IEnumerable {
            public TestEntity(IDataReader reader) {
                Guid = reader.GetGuid(reader.GetOrdinal("guid"));
                Dt = reader.GetDateTime(reader.GetOrdinal("dt"));
            }

            public TestEntity(Guid guid, DateTime dt) {
                Guid = guid;
                Dt = dt;
            }

            public Guid Guid { get; set; }

            public DateTime Dt { get; set; }

            public IEnumerator GetEnumerator() {
                yield return Guid;
                yield return Dt;
            }
        }

        [Test]
        public void TestSelect() {
            // Arrange
            var entities = new List<TestEntity> {
                new TestEntity(Guid.NewGuid(), DateTime.Now),
                new TestEntity(Guid.NewGuid(), DateTime.Now),
                new TestEntity(Guid.NewGuid(), DateTime.Now),
                new TestEntity(Guid.NewGuid(), DateTime.Now)
            };
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("TRUNCATE TABLE guid_test").ExecuteNonQuery();
                cnn.CreateCommand("INSERT INTO guid_test (guid, dt) VALUES @bulk").AddParameter("bulk", DbType.Object, entities).ExecuteNonQuery();
            }

            //Act
            var result = new List<TestEntity>();
            using (var cnn = ConnectionHandler.GetConnection()) {
                using (var reader = cnn.CreateCommand("SELECT guid, dt FROM guid_test;").ExecuteReader()) {
                    do {
                        while (reader.Read())
                            result.Add(new TestEntity(reader));
                    } while (reader.NextResult());
                }
            }

            //Assert 
            Assert.AreEqual(entities.Count, result.Count);
            foreach (var item in entities)
                Assert.AreEqual(1, result.Count(r => r.Guid == item.Guid));
        }

        [Test]
        public void TestParameter() {
            // Arrange
            var entity = new TestEntity(Guid.NewGuid(), DateTime.Now);
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("INSERT INTO guid_test (guid, dt) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] {entity}).ExecuteNonQuery();
            }

            //Act
            var result = new List<TestEntity>();
            using (var cnn = ConnectionHandler.GetConnection()) {
                using (var reader = cnn.CreateCommand("SELECT guid, dt FROM guid_test WHERE guid = @guid;").AddParameter("guid", DbType.Guid, entity.Guid).ExecuteReader()) {
                    do {
                        while (reader.Read())
                            result.Add(new TestEntity(reader));
                    } while (reader.NextResult());
                }
            }

            //Assert 
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(1, result.Count(r => r.Guid == entity.Guid));
        }
    }
}
using System;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test; 

[TestFixture]
public class Test_98_TZ {
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_98_dt64").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_98_dt64 (k Date, dt64 DateTime64(5), dt64tz DateTime64(5,'Europe/Moscow'))  ENGINE = MergeTree() PARTITION BY k ORDER BY (dt64,dt64tz)").ExecuteNonQuery();
        }

        Thread.Sleep(1000);
    }

    [Test]
    public void TestRoundtripLiteral() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_98_dt64 (k, dt64, dt64tz) VALUES ('2020-01-02','2020-01-01 00:00:00','2020-01-01 00:00:00')").ExecuteNonQuery();
            DateTime noOffset = DateTime.Now, offset = noOffset, b = new DateTime(2020, 01, 01, 0, 0, 0);
            cnn.CreateCommand("SELECT dt64, dt64tz FROM test_98_dt64 WHERE k='2020-01-02'").ExecuteReader().ReadAll(
                r => {
                    noOffset = r.GetDateTime(0);
                    offset = r.GetDateTime(1);
                }
            );
            Assert.Less(Math.Abs((noOffset - b).TotalMilliseconds), 1);
            Assert.Less(Math.Abs((offset - b).TotalMilliseconds), 1);
        }
    }

    [Test]
    public void TestRoundtripParameter() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_98_dt64 (k, dt64, dt64tz) VALUES ('2020-01-03',@d1,@d2)").AddParameter("d1", DbType.DateTime, new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc)).AddParameter("d2", DbType.DateTime, new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc))
               .ExecuteNonQuery();
            DateTime noOffset = DateTime.Now, offset = noOffset, b = new DateTime(2020, 01, 01, 0, 0, 0);
            cnn.CreateCommand("SELECT dt64, dt64tz FROM test_98_dt64 WHERE k='2020-01-03'").ExecuteReader().ReadAll(
                r => {
                    noOffset = r.GetDateTime(0);
                    offset = r.GetDateTime(1);
                }
            );
            Assert.Less(Math.Abs((noOffset - b).TotalMilliseconds), 1);
            Assert.Less(Math.Abs((offset - b).TotalMilliseconds), 1);
        }
    }
}

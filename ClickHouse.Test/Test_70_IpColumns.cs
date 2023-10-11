using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class Test_70_IpColumns {
    [OneTimeSetUp]
    public void CreateStructures() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("DROP TABLE IF EXISTS test_70_ip").ExecuteNonQuery();
            cnn.CreateCommand("CREATE TABLE test_70_ip (k Int32, ip4 IPv4, ip6 IPv6) ENGINE = Memory").ExecuteNonQuery();
        }

        Thread.Sleep(1000);
    }

    [Test]
    public void TestInsertBulk() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_70_ip (k, ip4, ip6) VALUES @bulk").AddParameter("bulk", DbType.Object, new object[] { new object[] { 1, IPAddress.Parse("127.0.0.1"), IPAddress.Parse("::1") } }).ExecuteNonQuery();
        }

        var values = SelectValues(1);
        Assert.True(values[0].Equals(IPAddress.Loopback));
        Assert.True(values[1].Equals(IPAddress.IPv6Loopback));
    }

    [Test]
    public void TestInsertLiteral() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_70_ip (k, ip4, ip6) VALUES (2,'127.0.0.1','::1')").ExecuteNonQuery();
        }

        var values = SelectValues(2);
        Assert.True(values[0].Equals(IPAddress.Loopback));
        Assert.True(values[1].Equals(IPAddress.IPv6Loopback));
    }

    [Test]
    public void TestInsertLiteralParameter() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            cnn.CreateCommand("INSERT INTO test_70_ip (k,ip4, ip6) VALUES (3,@p1, @p2)").AddParameter("p1", DbType.Binary, IPAddress.Parse("127.0.0.1")).AddParameter("p2", DbType.Binary, IPAddress.Parse("::1")).ExecuteNonQuery();
        }

        var values = SelectValues(3);
        Assert.True(values[0].Equals(IPAddress.Loopback));
        Assert.True(values[1].Equals(IPAddress.IPv6Loopback));
    }

    private List<IPAddress> SelectValues(int k) {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var values = new List<IPAddress>();
            using (var cmd = cnn.CreateCommand("SELECT ip4, ip6 FROM test_70_ip WHERE k=@k")) {
                cmd.AddParameter("k", k);
                using (var reader = cmd.ExecuteReader()) {
                    reader.ReadAll(
                        r => {
                            values.Add((IPAddress)r.GetValue(0));
                            values.Add((IPAddress)r.GetValue(1));
                        }
                    );
                }
            }

            return values;
        }
    }
}

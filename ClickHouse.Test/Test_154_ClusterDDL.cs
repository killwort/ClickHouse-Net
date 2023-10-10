using NUnit.Framework;

namespace ClickHouse.Test; 

[TestFixture]
public class Test_154_ClusterDDL {
    [Test]
    public void TestCreateTableOnCluster() {
        using (var cnn = ConnectionHandler.GetConnection(ConnectionHandler.ClusterTLSConnectionString)) {
            var cmd = cnn.CreateCommand("DROP TABLE IF EXISTS cl_test ON CLUSTER fbcluster");
            cmd.ExecuteNonQuery();
        }

        using (var cnn = ConnectionHandler.GetConnection(ConnectionHandler.ClusterTLSConnectionString)) {
            var cmd = cnn.CreateCommand("CREATE TABLE cl_test ON CLUSTER fbcluster (date Date)  ENGINE=MergeTree() ORDER BY date PARTITION BY date");
            cmd.ExecuteNonQuery();
        }
    }
}
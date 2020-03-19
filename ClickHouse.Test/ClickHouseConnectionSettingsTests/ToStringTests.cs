using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test.ClickHouseConnectionSettingsTests {
    [TestFixture]
    public class ToStringTests {
        [Test]
        public void ChangePropertyValue() {
            const string connectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=clickhouse;Port=9000;User=default;Password=;SocketTimeout=600000;Database=Test;";
            var settings = new ClickHouseConnectionSettings(connectionString) {Database = "New"};

            Assert.AreEqual(
                "Async=\"False\";BufferSize=\"4096\";ApacheBufferSize=\"0\";SocketTimeout=\"600000\";ConnectionTimeout=\"1000\";DataTransferTimeout=\"1000\";KeepAliveTimeout=\"1000\";TimeToLiveMillis=\"0\";DefaultMaxPerRoute=\"0\";MaxTotal=\"0\";Host=\"clickhouse\";Port=\"9000\";MaxCompressBufferSize=\"0\";MaxParallelReplicas=\"0\";Priority=\"0\";Database=\"New\";Compress=\"True\";Compressor=\"lz4\";CheckCompressedHash=\"False\";Decompress=\"False\";Extremes=\"False\";MaxThreads=\"0\";MaxExecutionTime=\"0\";MaxBlockSize=\"0\";MaxRowsToGroupBy=\"0\";User=\"default\";Password=\"\";DistributedAggregationMemoryEfficient=\"False\";MaxBytesBeforeExternalGroupBy=\"0\";MaxBytesBeforeExternalSort=\"0\";",
                settings.ToString()
            );
        }

        [Test]
        public void ShouldConvertIntoConnectionStringAndBack() {
            const string connectionString = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=clickhouse;Port=9000;User=default;Password=;SocketTimeout=600000;Database=Test;";
            var expectedSettings = new ClickHouseConnectionSettings(connectionString);
            var actualSettings = new ClickHouseConnectionSettings(expectedSettings.ToString());

            Assert.AreEqual(expectedSettings.BufferSize, actualSettings.BufferSize);
            Assert.AreEqual(expectedSettings.SocketTimeout, actualSettings.SocketTimeout);
            Assert.AreEqual(expectedSettings.Host, actualSettings.Host);
            Assert.AreEqual(expectedSettings.Port, actualSettings.Port);
            Assert.AreEqual(expectedSettings.Database, actualSettings.Database);
            Assert.AreEqual(expectedSettings.Compress, actualSettings.Compress);
            Assert.AreEqual(expectedSettings.Compressor, actualSettings.Compressor);
            Assert.AreEqual(expectedSettings.CheckCompressedHash, actualSettings.CheckCompressedHash);
            Assert.AreEqual(expectedSettings.User, actualSettings.User);
            Assert.AreEqual(expectedSettings.Password, actualSettings.Password);
        }
    }
}
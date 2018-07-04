using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Ado;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ClickHouse.Test.ClickHouseConnectionSettingsTests
{
    [TestClass]
    public class ToStringTests
    {
        [TestMethod]
        public void ShouldConvertIntoConnectionStringAndBack()
        {
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

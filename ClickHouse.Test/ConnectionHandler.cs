using ClickHouse.Ado;

namespace ClickHouse.Test {
    internal class ConnectionHandler {
        public static ClickHouseConnection GetConnection(
            string cstr = "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123") {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }
    }
}

using ClickHouse.Ado;

namespace ClickHouse.Test {
    internal class ConnectionHandler {
        public static ClickHouseConnection GetConnection(
            string cstr = "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=file-server;Port=9000;Database=default;User=test;Password=123") {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }
    }
}
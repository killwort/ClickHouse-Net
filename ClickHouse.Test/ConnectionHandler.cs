using ClickHouse.Ado;

namespace ClickHouse.Test;

internal class ConnectionHandler {
    internal const string SimpleConnectionString = "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123";
    internal const string TestDataConnectionString = "Compress=True;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=dev_fbo;User=andreya;Password=123";
    internal const string ClusterTLSConnectionString = "Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=true;Compressor=lz4;Host=b.clickhouse.flippingbook.com;Port=9440;Database=testing;User=andreya;Password=123";

    public static ClickHouseConnection GetConnection(string cstr = SimpleConnectionString) {
        var settings = new ClickHouseConnectionSettings(cstr);
        var cnn = new ClickHouseConnection(settings);
        cnn.Open();
        return cnn;
    }
}
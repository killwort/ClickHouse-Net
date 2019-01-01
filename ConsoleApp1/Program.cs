using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Ado;
using System.Data;


namespace ConsoleApp1
{

    class Program
    {
        static ClickHouseConnection chc = null;
        static ClickHouseCommand cmd = null;
        static private ClickHouseConnection GetConnection(string cstr="Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=192.168.1.120;Port=9000;Database=stock18;User=default;")
        //(string cstr = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123")
        {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }
        static void Main(string[] args)
        {
            var chc = GetConnection();
            cmd = chc.CreateCommand(
                "insert into tw18_11_12 (BidBuy, BidSale, Volume) values @bulk"
                );
            cmd.Parameters.Add(new ClickHouseParameter
            {
                DbType = DbType.Object,
                ParameterName = "bulk",
                Value = null
            });
            cmd.Parameters["bulk"].Value = new[]
            {
                new object[] { 9453.0, 1.0, 999 },
                new object[] { null, 0.0, 0},
                new object[] { 19453.0, 2.0, 3999 }
            };
            cmd.ExecuteNonQuery();        
        }
    }
}

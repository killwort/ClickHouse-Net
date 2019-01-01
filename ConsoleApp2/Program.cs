using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Ado;
using System.Data;
using System.Collections;

namespace ConsoleApp1
{

    class Program
    {
        static ClickHouseConnection chc = null;
        static ClickHouseCommand cmd = null;
        static private ClickHouseConnection GetConnection(string cstr = "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=192.168.1.120;Port=9000;Database=stock18;User=default;")
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
                "select BidBuy from tw18_11_12"
                );

            // fill the list to insert
            var list = new List<MyPersistableObject>();

            using (var reader = cmd.ExecuteReader())
            {
                reader.ReadAll(x =>
                {

                    for (var i = 0; i < x.FieldCount; i++)
                    {
                        var v = x.GetValue(i);
                        
                        var mpo = new MyPersistableObject { MySingleField = (Nullable<Double>)v };

                        list.Add(mpo);
                    }
                });
                //PrintData(reader);
            }
        }
        class MyPersistableObject : IEnumerable
        {
            public Nullable<Double> MySingleField;

            //Count and order of returns must match column order in SQL INSERT
            public IEnumerator GetEnumerator()
            {
                yield return MySingleField;
            }
        }

        //... somewhere elsewhere ...
        



        private static void PrintData(IDataReader reader)
        {
            do
            {
                Console.Write("Fields: ");
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write("{0}:{1} ", reader.GetName(i), reader.GetDataTypeName(i));
                }
                Console.WriteLine();
                while (reader.Read())
                {
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var val = reader.GetValue(i);
                        if (val.GetType().IsArray)
                        {
                            Console.Write('[');
                            Console.Write(string.Join(", ", ((IEnumerable)val).Cast<object>()));
                            Console.Write(']');
                        }
                        else
                        {
                            Console.Write(val);
                        }
                        Console.Write(", ");
                    }
                    Console.WriteLine();
                }
                Console.WriteLine();
            } while (reader.NextResult());
        }

    }
}

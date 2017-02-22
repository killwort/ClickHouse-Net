using System;
using System.Data;
using ClickHouse.Ado;

namespace ClickHouse.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var settings =new ClickHouseConnectionSettings("Compress=False;Compressor=lz4;Host=34.197.206.226;Port=9000;Database=default;User=andreya;Password=123");
            var cnn=new ClickHouseConnection(settings);
            cnn.Open();
            using (var reader = cnn.CreateCommand("SELECT * FROM test_data").ExecuteReader())
            {
                do
                {
                    Console.Write("Fields: ");
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        Console.Write("{0}:{1} ",reader.GetName(i),reader.GetDataTypeName(i));
                    }
                    Console.WriteLine();
                    while (reader.Read())
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write(reader.GetValue(i));
                            Console.Write(", ");
                        }
                        Console.WriteLine();
                    }
                    Console.WriteLine();
                } while (reader.NextResult());
            }
            var cmd=cnn.CreateCommand("INSERT INTO test_data (date,time,user_id,email,data)values @bulk;");
            cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName="bulk",
                    Value=new[]
                    {
                        new object[] {DateTime.Now,DateTime.Now,1ul,"aaaa@bbb.com","dsdsds"},
                        new object[] {DateTime.Now.AddHours(-1),DateTime.Now.AddHours(-1),2ul,"zzzz@xxx.com","qwqwqwq"},
                    }
                });
            cmd.ExecuteNonQuery();
        }
    }
}

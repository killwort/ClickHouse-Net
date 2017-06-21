using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using ClickHouse.Ado;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ClickHouse.Test
{
    [TestClass]
    public class SimpleTests
    {
        private ClickHouseConnection GetConnection(string cstr= "Compress=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123")
        {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }

        [TestMethod]
        public void SelectFromArray()
        {
            using (var cnn = GetConnection())
            using (var reader = cnn.CreateCommand("SELECT * FROM array_test").ExecuteReader())
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
                                Console.Write(string.Join(", ", ((IEnumerable) val).Cast<object>()));
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
        [TestMethod]
        public void TestInsertNestedColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO nest_test (date,x, values.name,values.value)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now, 1, new[] {"aaaa@bbb.com", "awdasdas"}, new[] {"dsdsds", "dsfdsds"}},
                        new object[] {DateTime.Now.AddHours(-1), 2, new string[0], new string[0]},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now, 1, new[] {"aaaa@bbb.com", "awdasdas"}},
                        new object[] {DateTime.Now.AddHours(-1), 2, new string[0]},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnConst()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,['a','b','c'])");
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnParam()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,@p)");
                cmd.AddParameter("p", new[] {"aaaa@bbb.com", "awdasdas"});
                cmd.ExecuteNonQuery();
            }
        }
    }
}

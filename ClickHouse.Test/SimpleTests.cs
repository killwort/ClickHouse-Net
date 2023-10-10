using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

[TestFixture]
public class SimpleTests {
    private static void PrintData(IDataReader reader) {
        do {
            Console.Write("Fields: ");
            for (var i = 0; i < reader.FieldCount; i++) Console.Write("{0}:{1} ", reader.GetName(i), reader.GetDataTypeName(i));
            Console.WriteLine();
            while (reader.Read()) {
                for (var i = 0; i < reader.FieldCount; i++) {
                    var val = reader.GetValue(i);
                    if (val.GetType().IsArray) {
                        Console.Write('[');
                        Console.Write(string.Join(", ", ((IEnumerable)val).Cast<object>()));
                        Console.Write(']');
                    } else {
                        Console.Write(val);
                    }

                    Console.Write(", ");
                }

                Console.WriteLine();
            }

            Console.WriteLine();
        } while (reader.NextResult());
    }

    [Test]
    public void DecimalParam() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
            cmd.AddParameter("d", DbType.Decimal, 666m);
            cmd.ExecuteNonQuery();
            cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
            cmd.AddParameter("d", DbType.Decimal, -666m);
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void TestSsl() {
        using (var cnn = ConnectionHandler.GetConnection("Compress=False;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Encrypt=True;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9440;Database=default;User=andreya;Password=123")) {
            var cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
            cmd.AddParameter("d", DbType.Decimal, 666m);
            cmd.ExecuteNonQuery();
            cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
            cmd.AddParameter("d", DbType.Decimal, -666m);
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void SelectDecimal() {
        using (var cnn = ConnectionHandler.GetConnection())
        using (var cmd = cnn.CreateCommand("SELECT date,dec1,dec2,dec3 FROM decimal_test")) {
            using (var reader = cmd.ExecuteReader()) {
                PrintData(reader);
            }
        }
    }

    [Test]
    public void SelectFromArray() {
        using (var cnn = ConnectionHandler.GetConnection())
        using (var reader = cnn.CreateCommand("SELECT * FROM array_test").ExecuteReader()) {
            PrintData(reader);
        }
    }

    [Test]
    public void SelectIn() {
        using (var cnn = ConnectionHandler.GetConnection())
        using (var cmd = cnn.CreateCommand("SELECT * FROM `test_data` WHERE user_id IN (@values)")) {
            cmd.Parameters.Add("values", DbType.UInt64, new[] { 1L, 2L, 3L });
            using (var reader = cmd.ExecuteReader()) {
                PrintData(reader);
            }
        }
    }

    [Test]
    public void TestBadNullableType() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("select 1,1,toNullable(max(datetime)), dumpColumnStructure(toNullable(max(datetime))) from default.nullable_date_time");
            using (var reader = cmd.ExecuteReader()) {
                reader.ReadAll(r => { Assert.True(r.IsDBNull(2)); });
            }
        }
    }

    [Test]
    public void TestBadReturnType() {
        using (var cnn = ConnectionHandler.GetConnection(ConnectionHandler.TestDataConnectionString)) {
            var cmd = cnn.CreateCommand();
            cmd.CommandText = @"SELECT 
   uniqCombinedMerge(uniq_links)                                                              uniq_links,
       groupUniqArrayMerge(uniq_pages)                                                            uniq_pages,   
   sumMerge(download) > 0                                                                     download
FROM page_view_aggregated_tracked_links_summary_v3
WHERE 1 = 1
    AND tracked_link_id = 2855
GROUP BY tracked_link_id";
            var reader = cmd.ExecuteReader();
            reader.ReadAll(
                rowReader => {
                    var rowData = new object[rowReader.FieldCount];

                    for (var i = 0; i < rowData.Length; i++)
                        rowData[i] = reader[i];
                }
            );
        }
    }

    [Test]
    public void TestChecksumError() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var sql = "insert into vince_test(fakedate, csa, server) values('2017-05-17', 'CSA_CPTY1233', 0)";
            cnn.CreateCommand(sql).ExecuteNonQuery();
        }
    }

    [Test]
    public void TestGuidByteOrder() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            try {
                cnn.CreateCommand("DROP TABLE guid_test").ExecuteNonQuery();
            } catch {
            }

            cnn.CreateCommand("CREATE TABLE guid_test (guid UUID,name String) ENGINE = MergeTree() ORDER BY (guid, name)").ExecuteNonQuery();

            cnn.CreateCommand("insert into guid_test values @bulk").AddParameter(
                "bulk",
                DbType.Object,
                new List<IList> {
                    new ArrayList {
                        Guid.Parse("dca0e161-9503-41a1-9de2-18528bfffe88"),
                        "Bulk insert"
                    }
                }
            ).ExecuteNonQuery();
            object g = null;
            using (var reader = cnn.CreateCommand("SELECT * FROM guid_test").ExecuteReader()) {
                reader.ReadAll(r => g = r.GetValue(0));
            }

            Assert.IsNotNull(g);
            Assert.IsTrue(g is Guid);
            Assert.AreEqual("dca0e161-9503-41a1-9de2-18528bfffe88", ((Guid)g).ToString("D"));
        }
    }

    [Test]
    public void TestInsertArrayColumnBulk() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("INSERT INTO default.`super+` (date,email)values @bulk;");
            cmd.Parameters.Add(
                new ClickHouseParameter {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[] { new object[] { DateTime.Now, "aaaa@bbb.com" }, new object[] { DateTime.Now.AddHours(-1), "" } }
                }
            );
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertArrayColumnConst() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,['a','b','c'])");
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertArrayColumnParam() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,@p)");
            cmd.AddParameter("p", new[] { "aaaa@bbb.com", "awdasdas" });
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void TestInsertNestedColumnBulk() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand("INSERT INTO nest_test (date,x, values.name,values.value)values @bulk;");
            cmd.Parameters.Add(
                new ClickHouseParameter {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[] { new object[] { DateTime.Now, 1, new[] { "aaaa@bbb.com", "awdasdas" }, new[] { "dsdsds", "dsfdsds" } }, new object[] { DateTime.Now.AddHours(-1), 2, new string[0], new string[0] } }
                }
            );
            cmd.ExecuteNonQuery();
        }
    }

    [Test]
    public void TestPerfromance() {
        using (var cnn = ConnectionHandler.GetConnection()) {
            var cmd = cnn.CreateCommand(
                "SELECT date, time, recordId, parentId, rootId, relatedUser, successState, initiatorType, initiatorPersonSsoId, initiatorPersonToken, initiatorPersonIp, initiatorServiceServer, initiatorServiceService, initiatorServiceDN, reporterType, reporterPersonSsoId, reporterPersonToken, reporterPersonIp, reporterServiceServer, reporterServiceService, reporterServiceDN, type, parameters.name,parameters.value,objectType,objectServer,objectIdentity,objectDescription FROM dev_audit.audit_actions WHERE parentId = 0 AND date> '2017-05-22' ORDER BY time DESC LIMIT 0,10"
            );
            cmd.AddParameter("p", new[] { "aaaa@bbb.com", "awdasdas" });
            var list = new List<List<object>>();
            var times = new List<TimeSpan>();
            var sw = new Stopwatch();
            sw.Start();
            using (var reader = cmd.ExecuteReader()) {
                times.Add(sw.Elapsed);
                sw.Restart();
                reader.ReadAll(
                    x => {
                        var rowList = new List<object>();
                        for (var i = 0; i < x.FieldCount; i++)
                            rowList.Add(x.GetValue(i));
                        list.Add(rowList);
                        times.Add(sw.Elapsed);
                        sw.Restart();
                    }
                );
            }

            sw.Stop();
        }
    }

    //
}
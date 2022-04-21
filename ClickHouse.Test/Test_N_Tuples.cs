using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_N_Tuples {
        [OneTimeSetUp]
        public void CreateStructures() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                cnn.CreateCommand("DROP TABLE IF EXISTS test_n_tuple").ExecuteNonQuery();
                cnn.CreateCommand("CREATE TABLE test_n_tuple (k Date, data Tuple(String, Int, Int), dataarray Array(Tuple(String, Int, Int)))  ENGINE = MergeTree(k, (data,dataarray), 8192)").ExecuteNonQuery();
            }

            Thread.Sleep(1000);
        }



        [Test]
        public void TestRoundtrip() {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO test_n_tuple (k, data,dataarray) VALUES @bulk");
                cmd.AddParameter("bulk", new[] { new object[] { new DateTime(2020, 01, 02), Tuple.Create("string", 1, 2), new []
                {
                    Tuple.Create("3", 3, 3),
                    Tuple.Create("3", 3, 3),
                    Tuple.Create("3", 3, 3)
                } } });
                cmd.ExecuteNonQuery();
                Tuple<string, int, int> val = null;
                Tuple<string, int, int>[] arr = null;
                cnn.CreateCommand("SELECT data,dataarray FROM test_n_tuple WHERE k='2020-01-02'").ExecuteReader().ReadAll(r =>
                {
                    val = (Tuple<string, int, int>)r.GetValue(0);
                    arr = (Tuple<string, int, int>[])r.GetValue(1);
                });
                
                Assert.IsNotNull(val);
                Assert.AreEqual("string", val.Item1);
                Assert.AreEqual(1,val.Item2);
                Assert.AreEqual(2, val.Item3);
                Assert.IsNotNull(arr);
                Assert.AreEqual(3, arr.Length);
                for (var i = 0; i < arr.Length; i++)
                {
                    Assert.IsNotNull(arr[i]);
                    Assert.AreEqual("3", arr[i].Item1);
                    Assert.AreEqual(3, arr[i].Item2);
                    Assert.AreEqual(3, arr[i].Item3);
                }
            }
        }
    }
}

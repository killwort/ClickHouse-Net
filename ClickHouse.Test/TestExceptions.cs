using System.Collections.Generic;
using System.Data;
using System.Linq;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test
{
    [TestFixture]
    public class TestExceptions
    {
        [Test, Timeout(5000)]
        public void TestSelectWithMemoryLimitException()
        {
            using (var cnn = ConnectionHandler.GetConnection())
            {
                cnn.CreateCommand("SET max_memory_usage = 3000").ExecuteNonQuery();

                Assert.Throws<ClickHouseException>(() =>
                {
                    var data = ReadRawData(cnn.CreateCommand("SELECT groupArray(number) FROM system.numbers LIMIT 1000000000")).ToList();
                });
            }      
        }

        private IEnumerable<IDataRecord> ReadRawData(ClickHouseCommand command)
        {
            using (var reader = command.ExecuteReader())
                while (reader.NextResult())
                    while (reader.Read())
                        yield return reader;
        }
    }
}
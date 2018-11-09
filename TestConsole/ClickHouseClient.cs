using ClickHouse.Ado;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ADBRO.EventHubClickHouseConsumer
{
    public class ClickHouseClient
    {
        private static readonly string _connectionString = "Host = localhost; Port=9000;Database=default;User=default";

        /// <summary>
        /// Performs bulk events insertion to ClickHouse.
        /// </summary>
        public static void InsertRange(IEnumerable<IEnumerable> collection, string sqlCommand)
        {
            using (var connection = new ClickHouseConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand(sqlCommand);

                command.Parameters.Add(new ClickHouseParameter
                {
                    ParameterName = "bulk",
                    Value = collection
                });

                command.ExecuteNonQuery();

                connection.Close();
            }
        }
    }
}

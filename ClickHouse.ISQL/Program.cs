using System;
using System.Collections.Generic;
using System.IO;
using ClickHouse.Ado;

namespace ClickHouse.Isql {
    internal class Program {
        private static int Help(string err = null) {
            if (err != null)
                Console.Error.WriteLine("Error: {0}", err);
            Console.Error.WriteLine(
                "Usage: clickhouse.isql [-host <hostname>] [-port <port>] [-user <username>] [-pass <password>] [-db <database>] [-output {TSV|TSVWithHeader|XML}] [-coalesce <coalescing value>] <query>"
            );
            return -1;
        }

        private static int Main(string[] args) {
            string host = "localhost", user = "", pass = "", query = null, db = "default";
            string coalesce = null;
            var port = 9000;
            var formatters = new Dictionary<OutputFormat, Func<Stream, Outputter>> {
                {OutputFormat.TSV, s => new TsvOutputter(s)},
                {OutputFormat.TSVWithHeader, s => new TsvWithHeaderOutputter(s)},
                {OutputFormat.XML, s => new XmlOutputter(s)}
            };
            var format = OutputFormat.TSV;
            for (var i = 0; i < args.Length; i++)
                if (!args[i].StartsWith("-") && !args[i].StartsWith("/"))
                    query = args[i];
                else
                    switch (args[i].TrimStart('-', '/').ToLower()) {
                        case "h":
                        case "host":
                            if (i == args.Length - 1)
                                return Help("Missing host parameter value.");
                            host = args[++i];
                            break;
                        case "p":
                        case "port":
                            if (i == args.Length - 1)
                                return Help("Missing port parameter value.");
                            port = int.Parse(args[++i]);
                            break;
                        case "u":
                        case "user":
                            if (i == args.Length - 1)
                                return Help("Missing user parameter value.");
                            user = args[++i];
                            break;
                        case "pass":
                        case "password":
                            if (i == args.Length - 1)
                                return Help("Missing password parameter value.");
                            pass = args[++i];
                            break;
                        case "d":
                        case "db":
                            if (i == args.Length - 1)
                                return Help("Missing db parameter value.");
                            db = args[++i];
                            break;
                        case "f":
                        case "format":
                            if (i == args.Length - 1)
                                return Help("Missing format parameter value.");
                            format = (OutputFormat) Enum.Parse(typeof(OutputFormat), args[++i], true);
                            break;
                        case "c":
                        case "coalesce":
                            if (i == args.Length - 1)
                                return Help("Missing coalesce parameter value.");
                            coalesce = args[++i];
                            break;
                    }

            if (string.IsNullOrWhiteSpace(query))
                return Help("Missing query to execute");
            var formatter = formatters[format](Console.OpenStandardOutput());
            formatter.Start();
            using (var cnn = new ClickHouseConnection($"Host={host};Port={port};User={user};Password={pass};Database={db}")) {
                cnn.Open();
                var hasOutput = false;
                using (var cmd = cnn.CreateCommand(query))
                using (var reader = cmd.ExecuteReader()) {
                    do {
                        formatter.ResultStart();
                        for (var i = 0; i < reader.FieldCount; i++)
                            formatter.HeaderCell(reader.GetName(i));
                        formatter.DataStart();
                        while (reader.Read()) {
                            formatter.RowStart();
                            for (var i = 0; i < reader.FieldCount; i++) {
                                formatter.ValueCell(reader.GetValue(i));
                                hasOutput = true;
                            }

                            formatter.RowEnd();
                        }

                        formatter.ResultEnd();
                    } while (reader.NextResult());
                }

                if (!hasOutput && coalesce != null) {
                    formatter.ResultStart();
                    formatter.HeaderCell("NULL");
                    formatter.DataStart();
                    formatter.RowStart();
                    formatter.ValueCell(coalesce);
                    formatter.RowEnd();
                    formatter.ResultEnd();
                }
            }

            formatter.End();
            return 0;
        }
    }
}
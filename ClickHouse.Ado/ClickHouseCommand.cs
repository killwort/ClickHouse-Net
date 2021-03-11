using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado {
    public class ClickHouseCommand : IDbCommand {
        private static readonly Regex ParamRegex = new Regex("[@:](?<n>([a-z_][a-z0-9_]*)|[@:])", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        public ClickHouseCommand() { }

        public ClickHouseCommand(ClickHouseConnection clickHouseConnection) => Connection = clickHouseConnection;

        public ClickHouseCommand(ClickHouseConnection clickHouseConnection, string text) : this(clickHouseConnection) => CommandText = text;

        public ClickHouseConnection Connection { get; set; }
        public ClickHouseParameterCollection Parameters { get; } = new ClickHouseParameterCollection();

        public void Dispose() { }

        public void Prepare() => throw new NotSupportedException();

        public void Cancel() => throw new NotSupportedException();

        IDbDataParameter IDbCommand.CreateParameter() => CreateParameter();

        IDbConnection IDbCommand.Connection { get => Connection; set => Connection = (ClickHouseConnection) value; }
        public IDbTransaction Transaction { get; set; }
        public CommandType CommandType { get; set; }
        IDataParameterCollection IDbCommand.Parameters => Parameters;
        public UpdateRowSource UpdatedRowSource { get; set; }

        public int ExecuteNonQuery() {
            Execute(true, Connection);
            return 0;
        }

        public IDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

        public IDataReader ExecuteReader(CommandBehavior behavior) {
            var tempConnection = Connection;
            Execute(false, tempConnection);
            return new ClickHouseDataReader(tempConnection, behavior);
        }

        public object ExecuteScalar() {
            object result = null;
            using (var reader = ExecuteReader()) {
                do {
                    if (!reader.Read()) continue;
                    result = reader.GetValue(0);
                } while (reader.NextResult());
            }

            return result;
        }

        public string CommandText { get; set; }
        public int CommandTimeout { get; set; }
        public ClickHouseParameter CreateParameter() => new ClickHouseParameter();

        private void Execute(bool readResponse, ClickHouseConnection connection) {
            if (connection.State != ConnectionState.Open) throw new InvalidOperationException("Connection isn't open");

            var insertParser = new Parser(new Scanner(new MemoryStream(Encoding.UTF8.GetBytes(CommandText))));
            insertParser.errors.errorStream = new StringWriter();
            insertParser.Parse();

            if (insertParser.errors.count == 0) {
                var xText = new StringBuilder("INSERT INTO ");
                xText.Append(insertParser.tableName);
                if (insertParser.fieldList != null) {
                    xText.Append("(");
                    insertParser.fieldList.Aggregate(xText, (builder, fld) => builder.Append(fld).Append(','));
                    xText.Remove(xText.Length - 1, 1);
                    xText.Append(")");
                }

                xText.Append(" VALUES");

                connection.Formatter.RunQuery(xText.ToString(), QueryProcessingStage.Complete, null, null, null, false);
                var schema = connection.Formatter.ReadSchema();
                if (insertParser.oneParam != null) {
                    if (Parameters[insertParser.oneParam].Value is IBulkInsertEnumerable bulkInsertEnumerable) {
                        var index = 0;
                        foreach (var col in schema.Columns)
                            col.Type.ValuesFromConst(bulkInsertEnumerable.GetColumnData(index++, col.Name, col.Type.AsClickHouseType(ClickHouseTypeUsageIntent.Generic)));
                    } else {
                        var table = ((IEnumerable) Parameters[insertParser.oneParam].Value).OfType<IEnumerable>();
                        var colCount = table.First().Cast<object>().Count();
                        if (colCount != schema.Columns.Count)
                            throw new FormatException($"Column count in parameter table ({colCount}) doesn't match column count in schema ({schema.Columns.Count}).");
                        var cl = new List<List<object>>(colCount);
                        for (var i = 0; i < colCount; i++)
                            cl.Add(new List<object>());
                        var index = 0;
                        cl = table.Aggregate(
                            cl,
                            (colList, row) => {
                                index = 0;
                                foreach (var cval in row) colList[index++].Add(cval);

                                return colList;
                            }
                        );
                        index = 0;
                        foreach (var col in schema.Columns) col.Type.ValuesFromConst(cl[index++]);
                    }
                } else {
                    if (schema.Columns.Count != insertParser.valueList.Count())
                        throw new FormatException($"Value count mismatch. Server expected {schema.Columns.Count} and query contains {insertParser.valueList.Count()}.");

                    var valueList = insertParser.valueList as List<Parser.ValueType> ?? insertParser.valueList.ToList();
                    for (var i = 0; i < valueList.Count; i++) {
                        var val = valueList[i];
                        if (val.TypeHint == Parser.ConstType.Parameter)
                            schema.Columns[i].Type.ValueFromParam(Parameters[val.StringValue]);
                        else
                            schema.Columns[i].Type.ValueFromConst(val);
                    }
                }

                connection.Formatter.SendBlocks(new[] {schema});
            } else {
                connection.Formatter.RunQuery(SubstituteParameters(CommandText), QueryProcessingStage.Complete, null, null, null, false);
            }

            if (!readResponse) return;
            connection.Formatter.ReadResponse();
        }

        private string SubstituteParameters(string commandText) =>
            ParamRegex.Replace(commandText, m => m.Groups["n"].Value == ":" || m.Groups["n"].Value == "@" ? m.Groups["n"].Value : Parameters[m.Groups["n"].Value].AsSubstitute());
    }
}

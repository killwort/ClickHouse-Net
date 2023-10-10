using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado; 

public class ClickHouseCommand : DbCommand, IDbCommand {
    private static readonly Regex ParamRegex = new("[@:](?<n>([a-z_][a-z0-9_]*)|[@:])", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    public ClickHouseCommand() { }

    public ClickHouseCommand(ClickHouseConnection clickHouseConnection) => Connection = clickHouseConnection;

    public ClickHouseCommand(ClickHouseConnection clickHouseConnection, string text) : this(clickHouseConnection) => CommandText = text;

    protected override DbConnection DbConnection { get; set; }
    protected override DbParameterCollection DbParameterCollection { get; }
    protected override DbTransaction DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public ClickHouseParameterCollection Parameters { get; } = new();

    public void Dispose() { }

    public override void Prepare() => throw new NotSupportedException();

    public override void Cancel() => throw new NotSupportedException();

    IDbDataParameter IDbCommand.CreateParameter() => new ClickHouseParameter();

    IDbConnection IDbCommand.Connection { get => Connection; set => Connection = (ClickHouseConnection)value; }
    public IDbTransaction Transaction { get; set; }
    public override CommandType CommandType { get; set; }
    IDataParameterCollection IDbCommand.Parameters => Parameters;
    public override UpdateRowSource UpdatedRowSource { get; set; }

    public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    IDataReader IDbCommand.ExecuteReader() => ExecuteDbDataReader(CommandBehavior.Default);

    IDataReader IDbCommand.ExecuteReader(CommandBehavior behavior) => ExecuteDbDataReader(behavior);

    public override object ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    public override string CommandText { get; set; }
    public override int CommandTimeout { get; set; }
    protected override DbParameter CreateDbParameter() => new ClickHouseParameter();
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cToken) {
        //For the weird folks who change Connection in between of Execute and actual reading the result.
        var tempConnection = (ClickHouseConnection)DbConnection;
        await Execute(false, tempConnection, cToken);
        return new ClickHouseDataReader(tempConnection, behavior);
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cToken) {
        await Execute(true, (ClickHouseConnection)DbConnection, cToken);
        return 0;
    }

    public override async Task<object> ExecuteScalarAsync(CancellationToken cToken) {
        object result = null;
        using (var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cToken)) {
            do {
                if (!await reader.ReadAsync(cToken)) continue;
                result = reader.GetValue(0);
            } while (await reader.NextResultAsync(cToken));
        }

        return result;
    }

    private async Task Execute(bool readResponse, ClickHouseConnection connection, CancellationToken cToken) {
        if (CommandTimeout > 0)
            cToken = CancellationTokenSource.CreateLinkedTokenSource(cToken, new CancellationTokenSource(TimeSpan.FromSeconds(CommandTimeout)).Token).Token;
        await connection.DialogueLock.WaitAsync(cToken);
        try {
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

                await connection.Formatter.RunQuery(xText.ToString(), QueryProcessingStage.Complete, null, null, null, false, cToken);
                var schema = await connection.Formatter.ReadSchema(cToken);
                if (insertParser.oneParam != null) {
                    if (Parameters[insertParser.oneParam].Value is IBulkInsertEnumerable bulkInsertEnumerable) {
                        var index = 0;
                        foreach (var col in schema.Columns)
                            col.Type.ValuesFromConst(bulkInsertEnumerable.GetColumnData(index++, col.Name, col.Type.AsClickHouseType(ClickHouseTypeUsageIntent.Generic)));
                    } else {
                        var table = ((IEnumerable)Parameters[insertParser.oneParam].Value).OfType<IEnumerable>();
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

                await connection.Formatter.SendBlocks(new[] { schema }, cToken);
            } else {
                await connection.Formatter.RunQuery(SubstituteParameters(CommandText), QueryProcessingStage.Complete, null, null, null, false, cToken);
            }

            if (!readResponse) return;
            await connection.Formatter.ReadResponse(cToken);
        } finally {
            if (readResponse) connection.DialogueLock.Release();
        }
    }

    private string SubstituteParameters(string commandText) => ParamRegex.Replace(commandText, m => m.Groups["n"].Value == ":" || m.Groups["n"].Value == "@" ? m.Groups["n"].Value : Parameters[m.Groups["n"].Value].AsSubstitute());
}

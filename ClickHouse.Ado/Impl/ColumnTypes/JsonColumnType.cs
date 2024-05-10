using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;

internal class JsonColumnType : ColumnType
{
    private string[] _data;

    public JsonColumnType()
    {
        _data = Array.Empty<string>();
    }

    public JsonColumnType(string[] data)
    {
        _data = data;
    }

    public override int Rows => _data.Length;
    internal override Type CLRType => typeof(string);

    // Reading a JSON object returns the data as a tuple.
    internal override Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) =>
        throw new NotSupportedException();

    public override void ValueFromConst(Parser.ValueType val)
    {
        if (val.TypeHint == Parser.ConstType.String)
        {
            var unescapedValue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
            _data = new[] { unescapedValue };
        }
        else
        {
            _data = new[] { val.StringValue };
        }
    }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "Object('json')";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken)
    {
        foreach (var d in _data)
        {
            // Clickhouse expects a byte indicating the type of the data when parsing Objects.
            // The possible values are 0 for Tuples and 1 for Strings
            await formatter.WriteByte(0b0000_0001, cToken);
            await formatter.WriteString(d, cToken);
        }
    }

    public override void ValueFromParam(ClickHouseParameter parameter)
    {
        _data = new[] { parameter.Value?.ToString() };
    }

    public override object Value(int currentRow) => _data[currentRow];

    public override long IntValue(int currentRow) => throw new InvalidCastException();

    public override void ValuesFromConst(IEnumerable objects)
    {
        _data = objects.Cast<string>().ToArray();
    }
}
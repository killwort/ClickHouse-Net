using System;
using System.Collections;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;
#pragma warning disable CS0618
internal class BooleanColumnType : ColumnType {
    public BooleanColumnType() { }

    public BooleanColumnType(bool[] data) => Data = data;

    public bool[] Data { get; private set; }

    public override int Rows => Data?.Length ?? 0;
    internal override Type CLRType => typeof(bool);

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        var bytes = await formatter.ReadBytes(rows, -1, cToken);
        Data = new bool[rows];
        for (var i = rows - 1; i >= 0; i--)
            Data[i] = bytes[i] != 0;
    }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "Bool";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        Debug.Assert(Rows == rows, "Row count mismatch!");
        var bytes = new byte[rows];
        for (var i = rows - 1; i >= 0; i--)
            bytes[i] = Data[i] ? (byte)1 : (byte)0;
        await formatter.WriteBytes(bytes, cToken);
    }

    public override void ValueFromConst(Parser.ValueType val) {
        if (val.TypeHint == Parser.ConstType.String)
            Data = new[] { string.Equals("true", ProtocolFormatter.UnescapeStringValue(val.StringValue), StringComparison.InvariantCultureIgnoreCase) };
        else if (val.TypeHint == Parser.ConstType.Number)
            Data = new[] { (int)Convert.ChangeType(val.StringValue, typeof(int)) != 0 };
        else
            throw new NotSupportedException();
    }

    public override void ValueFromParam(ClickHouseParameter parameter) {
        if (parameter.DbType == DbType.Int16 || parameter.DbType == DbType.Int32 || parameter.DbType == DbType.Int64 || parameter.DbType == DbType.UInt16 || parameter.DbType == DbType.UInt32 || parameter.DbType == DbType.UInt64 || parameter.DbType == DbType.Single ||
            parameter.DbType == DbType.Decimal || parameter.DbType == DbType.Double)
            Data = new[] { (int)Convert.ChangeType(parameter.Value, typeof(int)) != 0 };
        if (parameter.DbType == DbType.Boolean)
            Data = new[] { (bool)Convert.ChangeType(parameter.Value, typeof(bool)) };
        else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Boolean.");
    }

    public override object Value(int currentRow) => Data[currentRow];

    public override long IntValue(int currentRow) => Convert.ToInt64(Data[currentRow]);

    public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<bool>().ToArray();

    public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<bool?>().Select(x => x ?? false).ToArray();
}
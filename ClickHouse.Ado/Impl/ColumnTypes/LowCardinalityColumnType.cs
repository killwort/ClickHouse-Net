using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;

internal class LowCardinalityColumnType : ColumnType {
    private int _keySize;

    private int[] Indices;
    public LowCardinalityColumnType(ColumnType innerType) => InnerType = innerType;

    public override int Rows => Indices?.Length ?? 0;
    internal override Type CLRType => InnerType.CLRType;

    public ColumnType InnerType { get; }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"LowCardinality({InnerType.AsClickHouseType(usageIntent)})";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        // This is rather naive implementation of writing - without any deduplication, however who cares?
        // Clickhouse server will re-deduplicate inserted values anyway.
        await formatter.WriteBytes(BitConverter.GetBytes(1L), cToken);
        await formatter.WriteBytes(BitConverter.GetBytes(1538L), cToken);
        await formatter.WriteBytes(BitConverter.GetBytes((long)rows), cToken);
        await InnerType.Write(formatter, rows, cToken);
        await formatter.WriteBytes(BitConverter.GetBytes((long)rows), cToken);
        for (var i = 0; i < rows; i++)
            await formatter.WriteBytes(BitConverter.GetBytes(i), cToken);
    }

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        var version = BitConverter.ToInt64(await formatter.ReadBytes(8, -1, cToken), 0);
        if (version != 1)
            throw new NotSupportedException("Invalid LowCardinality dictionary version");
        var keyLength = BitConverter.ToInt64(await formatter.ReadBytes(8, -1, cToken), 0);
        _keySize = 1 << (byte)(keyLength & 0xff);
        if (_keySize < 0 || _keySize > 4) //LowCardinality with >4e9 keys? WTF???
            throw new NotSupportedException("Invalid LowCardinality key size");
        if (((keyLength >> 8) & 0xff) != 6)
            throw new NotSupportedException("Invalid LowCardinality key flags");
        var keyCount = BitConverter.ToInt64(await formatter.ReadBytes(8, -1, cToken), 0);
        await InnerType.Read(formatter, (int)keyCount, cToken);
        var valueCount = BitConverter.ToInt64(await formatter.ReadBytes(8, -1, cToken), 0);
        Indices = new int[rows];
        for (var i = 0; i < rows; i++) Indices[i] = BitConverter.ToInt32(await formatter.ReadBytes(_keySize, 4, cToken), 0);
    }

    public override void ValueFromConst(Parser.ValueType val) {
        InnerType.ValueFromConst(val);
        Indices = new int[InnerType.Rows];
    }

    public override void ValueFromParam(ClickHouseParameter parameter) {
        InnerType.ValueFromParam(parameter);
        Indices = new int[InnerType.Rows];
    }

    public override object Value(int currentRow) => InnerType.Value(Indices[currentRow]);

    public override long IntValue(int currentRow) => InnerType.IntValue(Indices[currentRow]);

    public override void ValuesFromConst(IEnumerable objects) {
        InnerType.NullableValuesFromConst(objects);
        Indices = new int[InnerType.Rows];
    }
}
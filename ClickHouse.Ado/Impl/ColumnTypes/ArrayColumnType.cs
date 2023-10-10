using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes; 

internal class ArrayColumnType : ColumnType {
    private int _outerRows;

    public ArrayColumnType(ColumnType innerType) {
        Offsets = new SimpleColumnType<ulong>();
        InnerType = innerType;
    }

    public ColumnType InnerType { get; }
    public SimpleColumnType<ulong> Offsets { get; }

    public override int Rows => _outerRows;

    internal override Type CLRType => InnerType.CLRType.MakeArrayType();

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        await Offsets.Read(formatter, rows, cToken);
        _outerRows = rows;
        var totalRows = Offsets.Data.Last();
        if (totalRows == 0) return;
        await InnerType.Read(formatter, (int)totalRows, cToken);
    }

    public override void ValueFromConst(Parser.ValueType val) {
        if (val.TypeHint == Parser.ConstType.Array) {
            InnerType.ValuesFromConst(val.ArrayValue.Select(x => Convert.ChangeType(x.StringValue, InnerType.CLRType)));
            Offsets.ValueFromConst(
                new Parser.ValueType {
                    TypeHint = Parser.ConstType.Number,
                    StringValue = InnerType.Rows.ToString()
                }
            );
        } else {
            throw new NotSupportedException();
        }
    }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Array({InnerType.AsClickHouseType(usageIntent)})";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        await Offsets.Write(formatter, rows, cToken);
        var totalRows = rows == 0 ? 0 : Offsets.Data.Last();
        await InnerType.Write(formatter, (int)totalRows, cToken);
    }

    public override void ValueFromParam(ClickHouseParameter parameter) {
        if (parameter.DbType == 0 || parameter.DbType == DbType.Object)
            ValuesFromConst(new[] { parameter.Value as IEnumerable });
        else throw new NotSupportedException();
    }

    public override object Value(int currentRow) {
        var start = currentRow == 0 ? 0 : Offsets.Data[currentRow - 1];
        var end = Offsets.Data[currentRow];
        var rv = Array.CreateInstance(InnerType.CLRType, (int)(end - start));
        for (var i = start; i < end; i++)
            rv.SetValue(InnerType.Value((int)i), (int)(i - start));
        return rv;
    }

    public override long IntValue(int currentRow) => throw new InvalidCastException();

    public override void ValuesFromConst(IEnumerable objects) {
        var offsets = new List<ulong>();
        var itemsPlain = new List<object>();
        ulong currentOffset = 0;
        foreach (var item in objects) {
            ulong itemCount = 0;
            foreach (var itemPart in (IEnumerable)item) {
                itemCount++;
                itemsPlain.Add(itemPart);
            }

            currentOffset += itemCount;
            offsets.Add(currentOffset);
        }

        Offsets.ValuesFromConst(offsets);
        InnerType.ValuesFromConst(itemsPlain);
        _outerRows = offsets.Count;
    }
}
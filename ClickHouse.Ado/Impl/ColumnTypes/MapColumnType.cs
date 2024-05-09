using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;

internal class MapColumnType : ColumnType
{
    private readonly ColumnType _keyType;
    private readonly ColumnType _valueType;

    private readonly MethodInfo _mapCreator;
    private readonly SimpleColumnType<ulong> _offsets = new();

    private int _outerRows;

    public MapColumnType(ColumnType keyType, ColumnType valueType)
    {
        _keyType = keyType;
        _valueType = valueType;

        _mapCreator = CLRType.GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Single(x => x.Name == "Add" && x.GetParameters().Length == 2);
    }

    public override int Rows => _outerRows;

    internal sealed override Type CLRType =>
        typeof(Dictionary<,>).MakeGenericType(_keyType.CLRType, _valueType.CLRType);

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken)
    {
        await _offsets.Read(formatter, rows, cToken);
        _outerRows = rows;

        var totalRows = _offsets.Data.Last();
        if (totalRows == 0)
        {
            return;
        }

        await _keyType.Read(formatter, (int)totalRows, cToken);
        await _valueType.Read(formatter, (int)totalRows, cToken);
    }

    public override void ValueFromConst(Parser.ValueType val) => throw new NotSupportedException();

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) =>
        $"Map({_keyType.AsClickHouseType(usageIntent)}, {_valueType.AsClickHouseType(usageIntent)})";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken)
    {
        await _offsets.Write(formatter, rows, cToken);
        var totalRows = rows == 0 ? 0 : _offsets.Data.Last();
        await _keyType.Write(formatter, (int)totalRows, cToken);
        await _valueType.Write(formatter, (int)totalRows, cToken);
    }

    public override void ValueFromParam(ClickHouseParameter parameter)
    {
        if (parameter.DbType is 0 or DbType.Object)
        {
            ValuesFromConst(new[] { parameter.Value as IEnumerable });
        }
        else
        {
            throw new NotSupportedException();
        }
    }


    public override object Value(int currentRow)
    {
        var start = currentRow == 0 ? 0 : _offsets.Data[currentRow - 1];
        var end = _offsets.Data[currentRow];

        var dict = Activator.CreateInstance(CLRType, args: (int)(end - start));

        for (var i = start; i < end; i++)
        {
            _mapCreator.Invoke(dict, new[] { _keyType.Value((int)i), _valueType.Value((int)i) });
        }

        return dict;
    }

    public override long IntValue(int currentRow) => throw new InvalidCastException();

    public override void ValuesFromConst(IEnumerable objects)
    {
        var offsets = new List<ulong>();
        var keys = new List<object>();
        var values = new List<object>();
        ulong currentOffset = 0;

        foreach (var row in objects)
        {
            if (row is IDictionary dict)
            {
                ulong itemCount = 0;
                foreach (DictionaryEntry entry in dict)
                {
                    keys.Add(entry.Key);
                    values.Add(entry.Value);
                    itemCount++;
                }

                currentOffset += itemCount;
                offsets.Add(currentOffset);
            }
            else
            {
                throw new InvalidOperationException("Expected an object implementing IDictionary.");
            }
        }

        _offsets.ValuesFromConst(offsets);
        _keyType.ValuesFromConst(keys);
        _valueType.ValuesFromConst(values);
        _outerRows = offsets.Count;
    }
}
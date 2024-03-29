﻿using System;
using System.Collections;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;

internal class NullColumnType : ColumnType {
    private int _rows;

    public override int Rows => _rows;
    internal override Type CLRType => typeof(object);

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        await new SimpleColumnType<byte>().Read(formatter, rows, cToken);
        _rows = rows;
    }

    public override void ValueFromConst(Parser.ValueType val) { }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "Null";

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        Debug.Assert(Rows == rows, "Row count mismatch!");
        await new SimpleColumnType<byte>(new byte[rows]).Write(formatter, rows, cToken);
    }

    public override void ValueFromParam(ClickHouseParameter parameter) { }

    public override object Value(int currentRow) => null;

    public override long IntValue(int currentRow) => 0;

    public override void ValuesFromConst(IEnumerable objects) { }

    public override void NullableValuesFromConst(IEnumerable objects) { }
}
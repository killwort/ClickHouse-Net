using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ColumnTypes;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado;

/// <summary>
///     Clickhouse specific data reader.
/// </summary>
public class ClickHouseDataReader : DbDataReader, IDataReader, IDisposable {
    private readonly CommandBehavior _behavior;
    private ClickHouseConnection _clickHouseConnection;

    private Block _currentBlock;
    private int _currentRow;

    internal ClickHouseDataReader(ClickHouseConnection clickHouseConnection, CommandBehavior behavior) {
        _clickHouseConnection = clickHouseConnection;
        _behavior = behavior;
        NextResult();
    }

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override bool HasRows => _currentBlock?.Rows > 0;

    /// <inheritdoc />
    public override string GetName(int i) => _currentBlock.Columns[i].Name;

    /// <inheritdoc />
    public override string GetDataTypeName(int i) {
        if (_currentBlock == null)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        return _currentBlock.Columns[i].Type.AsClickHouseType(ClickHouseTypeUsageIntent.Generic);
    }

    /// <inheritdoc />
    public override Type GetFieldType(int i) {
        if (_currentBlock == null)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        return _currentBlock.Columns[i].Type.CLRType;
    }

    /// <inheritdoc />
    public override object GetValue(int i) {
        if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        return _currentBlock.Columns[i].Type.Value(_currentRow);
    }

    /// <inheritdoc />
    public override int GetValues(object[] values) {
        if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        var n = Math.Max(values.Length, _currentBlock.Columns.Count);
        for (var i = 0; i < n; i++)
            values[i] = _currentBlock.Columns[i].Type.Value(_currentRow);
        return n;
    }

    /// <inheritdoc />
    public override int GetOrdinal(string name) {
        if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        return _currentBlock.Columns.FindIndex(x => x.Name == name);
    }

    /// <inheritdoc />
    public override bool GetBoolean(int i) => GetInt64(i) != 0;

    /// <inheritdoc />
    public override byte GetByte(int i) => (byte)GetInt64(i);

    /// <inheritdoc />
    public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();

    /// <inheritdoc />
    public override char GetChar(int i) => (char)GetInt64(i);

    /// <inheritdoc />
    public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();

    /// <inheritdoc />
    public override Guid GetGuid(int i) => (Guid)GetValue(i);

    /// <inheritdoc />
    public override short GetInt16(int i) => (short)GetInt64(i);

    /// <inheritdoc />
    public override int GetInt32(int i) => (int)GetInt64(i);

    /// <inheritdoc />
    public override long GetInt64(int i) {
        if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        return _currentBlock.Columns[i].Type.IntValue(_currentRow);
    }

    /// <inheritdoc />
    public override float GetFloat(int i) => Convert.ToSingle(GetValue(i));

    /// <inheritdoc />
    public override double GetDouble(int i) => Convert.ToDouble(GetValue(i));

    /// <inheritdoc />
    public override string GetString(int i) => GetValue(i).ToString();

    /// <inheritdoc />
    public override decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i));

    /// <inheritdoc />
    public override DateTime GetDateTime(int i) => Convert.ToDateTime(GetValue(i));

    object IDataRecord.this[int i] => GetValue(i);

    object IDataRecord.this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override bool IsDBNull(int i) {
        if (_currentBlock == null)
            throw new InvalidOperationException("Trying to read beyond end of stream.");

        var type = _currentBlock.Columns[i].Type as NullableColumnType;
        if (type != null)
            return type.IsNull(_currentRow);
        return false;
    }

    /// <inheritdoc />
    public override int FieldCount => _currentBlock.Columns.Count;

    /// <inheritdoc />
    public override void Close() => CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override bool NextResult() => NextResultAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override bool Read() => ReadAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override int Depth { get; } = 1;

    /// <inheritdoc />
    public override bool IsClosed => _clickHouseConnection == null;

    /// <inheritdoc />
    public override int RecordsAffected => _currentBlock.Rows;

    /// <inheritdoc />
    protected override void Dispose(bool disposing) {
        base.Dispose(disposing);
        Close();
    }
#if CORE_FRAMEWORK
    /// <inheritdoc />
    public override async ValueTask DisposeAsync() {
        await base.DisposeAsync();
        await CloseAsync();
    }
#endif
    /// <inheritdoc />
    public override IEnumerator GetEnumerator() {
        do {
            var values = new object[FieldCount];
            while (Read()) {
                GetValues(values);
                yield return values;
            }
        } while (NextResult());
    }

    /// <inheritdoc />
    public
#if CORE_FRAMEWORK
        override
#endif
        async Task CloseAsync() {
        if (_currentBlock != null && _clickHouseConnection != null)
            await _clickHouseConnection.Formatter.ReadResponse(new CancellationTokenSource(TimeSpan.FromSeconds(1)).Token);
        if (_clickHouseConnection != null && (_behavior & CommandBehavior.CloseConnection) != 0)
            _clickHouseConnection.Close();
        _clickHouseConnection?.DialogueLock.Release();
        _clickHouseConnection = null;
    }

    /// <inheritdoc />
    public override async Task<bool> NextResultAsync(CancellationToken cToken) {
        _currentRow = -1;

        try {
            _currentBlock = await _clickHouseConnection.Formatter.ReadBlock(cToken);
        } catch {
            _currentBlock = null;
            throw;
        }

        return _currentBlock != null;
    }

    /// <inheritdoc />
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) {
        if (_currentBlock == null)
            throw new InvalidOperationException("Trying to read beyond end of stream.");
        _currentRow++;
        if (_currentBlock.Rows <= _currentRow)
            return Task.FromResult(false);
        return Task.FromResult(true);
    }
}
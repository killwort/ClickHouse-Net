using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

// ReSharper disable UnusedMember.Global

namespace ClickHouse.Ado;

/// <summary>
///     Collection of <see cref="ClickHouseParameter" />.
/// </summary>
public class ClickHouseParameterCollection : DbParameterCollection, IEnumerable<ClickHouseParameter>, IDataParameterCollection {
    private readonly List<ClickHouseParameter> _parameters = new();

    /// <summary>
    ///     Gets parameter by its index.
    /// </summary>
    /// <param name="index">Index.</param>
    public new ClickHouseParameter this[int index] { get => _parameters[index]; set => _parameters[index] = value; }

    /// <summary>
    ///     Gets parameter by its name.
    /// </summary>
    /// <param name="parameterName">Parameter name.</param>
    public new ClickHouseParameter this[string parameterName] {
        get => _parameters.First(x => x.ParameterName == parameterName);
        set {
            value.ParameterName = parameterName;
            var index = _parameters.FindIndex(x => x.ParameterName == parameterName);
            if (index == -1)
                _parameters.Add(value);
            else
                _parameters[index] = value;
        }
    }

    /// <inheritdoc />
    public override void CopyTo(Array array, int index) => Array.Copy(this.ToArray(), 0, array, index, Math.Min(array.Length, Count));

    /// <inheritdoc />
    public override int Count => _parameters.Count;

    /// <inheritdoc />
    public override object SyncRoot { get; } = new();

    /// <inheritdoc />
    public override bool IsSynchronized => false;

    /// <inheritdoc />
    public override void Clear() => _parameters.Clear();

    /// <inheritdoc />
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    /// <inheritdoc />
    public override bool IsReadOnly => false;

    /// <inheritdoc />
    public override bool IsFixedSize => false;

    /// <inheritdoc />
    public override bool Contains(string parameterName) => _parameters.Exists(x => x.ParameterName == parameterName);

    /// <inheritdoc />
    public override int IndexOf(string parameterName) => _parameters.FindIndex(x => x.ParameterName == parameterName);

    /// <inheritdoc />
    public override void RemoveAt(string parameterName) => _parameters.RemoveAll(x => x.ParameterName == parameterName);

    int IList.Add(object value) {
        _parameters.Add((ClickHouseParameter)value);
        return _parameters.Count - 1;
    }

    bool IList.Contains(object value) => _parameters.Contains(value);

    int IList.IndexOf(object value) => _parameters.IndexOf((ClickHouseParameter)value);

    void IList.Insert(int index, object value) => _parameters.Insert(index, (ClickHouseParameter)value);

    void IList.Remove(object value) => _parameters.Remove((ClickHouseParameter)value);

    object IList.this[int index] { get => _parameters[index]; set => _parameters[index] = (ClickHouseParameter)value; }

    object IDataParameterCollection.this[string parameterName] { get => _parameters.First(x => x.ParameterName == parameterName); set => _parameters[_parameters.FindIndex(x => x.ParameterName == parameterName)] = (ClickHouseParameter)value; }

    IEnumerator<ClickHouseParameter> IEnumerable<ClickHouseParameter>.GetEnumerator() => _parameters.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _parameters.GetEnumerator();

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value) => this[parameterName] = (ClickHouseParameter)value;

    /// <inheritdoc />
    public override int Add(object value) {
        Add((ClickHouseParameter)value);
        return _parameters.Count - 1;
    }

    /// <inheritdoc />
    public override void AddRange(Array values) {
        foreach (var val in values)
            Add(val);
    }

    /// <inheritdoc />
    public override bool Contains(object value) => Contains((ClickHouseParameter)value);

    /// <inheritdoc />
    public override int IndexOf(object value) => IndexOf((ClickHouseParameter)value);

    /// <inheritdoc />
    public override void Insert(int index, object value) => Insert(index, (ClickHouseParameter)value);

    /// <inheritdoc />
    public override void Remove(object value) => Remove((ClickHouseParameter)value);

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value) => this[index] = (ClickHouseParameter)value;

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName) => this[parameterName];

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => this[index];

    /// <inheritdoc cref="Add(object)" />
    public void Add(ClickHouseParameter value) => _parameters.Add(value);

    /// <inheritdoc cref="Contains(object)" />
    public bool Contains(ClickHouseParameter value) => _parameters.Contains(value);

    /// <inheritdoc cref="IndexOf(object)" />
    public int IndexOf(ClickHouseParameter value) => _parameters.IndexOf(value);

    /// <inheritdoc cref="Insert(int,object)" />
    public void Insert(int index, ClickHouseParameter value) => _parameters.Insert(index, value);

    /// <inheritdoc cref="Remove(object)" />
    public void Remove(ClickHouseParameter value) => _parameters.Remove(value);

    /// <summary>
    ///     Adds named parameter with value to the collection.
    /// </summary>
    /// <param name="name">Parameter name.</param>
    /// <param name="value">Parameter value.</param>
    /// <returns><code>this</code> to chain calls.</returns>
    public ClickHouseParameter Add(string name, object value) {
        var p = new ClickHouseParameter {
            ParameterName = name,
            Value = value
        };
        Add(p);
        return p;
    }

    /// <summary>
    ///     Adds named parameter with type and value to the collection.
    /// </summary>
    /// <param name="name">Parameter name.</param>
    /// <param name="type">Parameter type.</param>
    /// <param name="value">Parameter value.</param>
    /// <returns><code>this</code> to chain calls.</returns>
    public ClickHouseParameter Add(string name, DbType type, object value) {
        var p = new ClickHouseParameter {
            ParameterName = name,
            DbType = type,
            Value = value
        };
        Add(p);
        return p;
    }
}

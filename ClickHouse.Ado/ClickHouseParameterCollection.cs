using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ClickHouse.Ado {
    public class ClickHouseParameterCollection : IEnumerable<ClickHouseParameter>, IDataParameterCollection {
        private readonly List<ClickHouseParameter> _parameters = new List<ClickHouseParameter>();

        public ClickHouseParameter this[int index] { get => _parameters[index]; set => _parameters[index] = value; }

        public ClickHouseParameter this[string parameterName] {
            get => _parameters.First(x => x.ParameterName == parameterName);
            set => _parameters[_parameters.FindIndex(x => x.ParameterName == parameterName)] = value;
        }

        public void CopyTo(Array array, int index) => Array.Copy(this.ToArray(), 0, array, index, Math.Min(array.Length, Count));

        public int Count => _parameters.Count;
        public object SyncRoot { get; } = new object();

        public bool IsSynchronized => false;

        public void Clear() => _parameters.Clear();

        public void RemoveAt(int index) => _parameters.RemoveAt(index);

        public bool IsReadOnly => false;
        public bool IsFixedSize => false;

        public bool Contains(string parameterName) => _parameters.Exists(x => x.ParameterName == parameterName);

        public int IndexOf(string parameterName) => _parameters.FindIndex(x => x.ParameterName == parameterName);

        public void RemoveAt(string parameterName) => _parameters.RemoveAll(x => x.ParameterName == parameterName);

        int IList.Add(object value) {
            _parameters.Add((ClickHouseParameter) value);
            return _parameters.Count - 1;
        }

        bool IList.Contains(object value) => _parameters.Contains(value);

        int IList.IndexOf(object value) => _parameters.IndexOf((ClickHouseParameter) value);

        void IList.Insert(int index, object value) => _parameters.Insert(index, (ClickHouseParameter) value);

        void IList.Remove(object value) => _parameters.Remove((ClickHouseParameter) value);

        object IList.this[int index] { get => _parameters[index]; set => _parameters[index] = (ClickHouseParameter) value; }

        object IDataParameterCollection.this[string parameterName] {
            get => _parameters.First(x => x.ParameterName == parameterName);
            set => _parameters[_parameters.FindIndex(x => x.ParameterName == parameterName)] = (ClickHouseParameter) value;
        }

        public IEnumerator<ClickHouseParameter> GetEnumerator() => _parameters.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _parameters.GetEnumerator();

        public void Add(ClickHouseParameter value) => _parameters.Add(value);

        public bool Contains(ClickHouseParameter value) => _parameters.Contains(value);

        public int IndexOf(ClickHouseParameter value) => _parameters.IndexOf(value);

        public void Insert(int index, ClickHouseParameter value) => _parameters.Insert(index, value);

        public void Remove(ClickHouseParameter value) => _parameters.Remove(value);

        public ClickHouseParameter Add(string name, object value) {
            var p = new ClickHouseParameter {
                ParameterName = name,
                Value = value
            };
            Add(p);
            return p;
        }

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
}
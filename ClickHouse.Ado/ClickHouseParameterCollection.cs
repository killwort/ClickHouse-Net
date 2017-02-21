using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ClickHouse.Ado
{
    public class ClickHouseParameterCollection : IEnumerable<ClickHouseParameter>, IDataParameterCollection
    {
        private List<ClickHouseParameter> _parameters = new List<ClickHouseParameter>();

        public IEnumerator<ClickHouseParameter> GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            Array.Copy(this.ToArray(), 0, array, index, Math.Min(array.Length, Count));
        }

        public int Count => _parameters.Count;
        public object SyncRoot { get; } = new object();

        public bool IsSynchronized => false;
        int IList.Add(object value)
        {
            _parameters.Add((ClickHouseParameter)value);
            return _parameters.Count - 1;
        }
        public void Add(ClickHouseParameter value)
        {
            _parameters.Add(value);
        }

        bool IList.Contains(object value)
        {
            return _parameters.Contains(value);
        }
        public bool Contains(ClickHouseParameter value)
        {
            return _parameters.Contains(value);
        }

        public void Clear()
        {
            _parameters.Clear();
        }

        int IList.IndexOf(object value)
        {
            return _parameters.IndexOf((ClickHouseParameter)value);
        }
        public int IndexOf(ClickHouseParameter value)
        {
            return _parameters.IndexOf(value);
        }

        void IList.Insert(int index, object value)
        {
            _parameters.Insert(index, (ClickHouseParameter)value);
        }
        public void Insert(int index, ClickHouseParameter value)
        {
            _parameters.Insert(index, value);
        }

        void IList.Remove(object value)
        {
            _parameters.Remove((ClickHouseParameter)value);
        }
        public void Remove(ClickHouseParameter value)
        {
            _parameters.Remove(value);
        }

        public void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        object IList.this[int index]
        {
            get { return _parameters[index]; }
            set { _parameters[index] = (ClickHouseParameter)value; }
        }
        public ClickHouseParameter this[int index]
        {
            get { return _parameters[index]; }
            set { _parameters[index] = value; }
        }

        object IDataParameterCollection.this[string parameterName]
        {
            get { return _parameters.First(x => x.ParameterName == parameterName); }
            set { _parameters[_parameters.FindIndex(x => x.ParameterName == parameterName)] = (ClickHouseParameter) value; }
        }

        public ClickHouseParameter this[string parameterName]
        {
            get { return _parameters.First(x => x.ParameterName == parameterName); }
            set { _parameters[_parameters.FindIndex(x => x.ParameterName == parameterName)] = value; }
        }

        public bool IsReadOnly => false;
        public bool IsFixedSize => false;
        public bool Contains(string parameterName)
        {
            return _parameters.Exists(x => x.ParameterName == parameterName);
        }

        public int IndexOf(string parameterName)
        {
            return _parameters.FindIndex(x => x.ParameterName == parameterName);
        }

        public void RemoveAt(string parameterName)
        {
            _parameters.RemoveAll(x => x.ParameterName == parameterName);
        }

        
    }
}
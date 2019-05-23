using System;
using System.Collections;
using System.Globalization;
using System.Linq;
#if !NETCOREAPP11
using System.Data;
#endif
using ClickHouse.Ado.Impl;

namespace ClickHouse.Ado
{
    public class ClickHouseParameter
#if !NETCOREAPP11
        : IDbDataParameter
#endif
    {
        public DbType DbType { get; set; }
#if !NETCOREAPP11

        ParameterDirection IDataParameter.Direction { get; set; }
        bool IDataParameter.IsNullable => false;
        string IDataParameter.SourceColumn { get; set; }
        DataRowVersion IDataParameter.SourceVersion { get; set; }
        byte IDbDataParameter.Precision { get; set; }
        byte IDbDataParameter.Scale { get; set; }
        int IDbDataParameter.Size { get; set; }
#endif
        public string ParameterName { get; set; }
        public object Value { get; set; }

        private string AsSubstitute(object val)
        {
            if (DbType == DbType.String
#if !NETCOREAPP11
                || DbType == DbType.AnsiString || DbType == DbType.StringFixedLength || DbType == DbType.AnsiStringFixedLength
#endif
                ||(DbType==0 && val is string)
            )
                if (!(val is string) && val is IEnumerable)
                    return string.Join(",", ((IEnumerable) val).Cast<object>().Select(AsSubstitute));
                else
                    return ProtocolFormatter.EscapeStringValue(val.ToString());
            if (DbType == DbType.DateTime
#if !NETCOREAPP11
                || DbType == DbType.DateTime2 || DbType == DbType.DateTime2
#endif
                || (DbType==0 && val is DateTime)
            )
                return $"'{(DateTime)val:yyyy-MM-dd HH:mm:ss}'";
            if (DbType == DbType.Date)
                return $"'{(DateTime)val:yyyy-MM-dd}'";
            if ((DbType != 0
#if !NETCOREAPP11
                 && DbType != DbType.Object
#endif
                ) && !(val is string) && val is IEnumerable)
            {
                return string.Join(",", ((IEnumerable)val).Cast<object>().Select(AsSubstitute));
            }
            if ((DbType==0
#if !NETCOREAPP11
                || DbType==DbType.Object
#endif
                ) && !(val is string) && val is IEnumerable )
            {
                return "[" + string.Join(",", ((IEnumerable) val).Cast<object>().Select(AsSubstitute)) + "]";
            }

            if (val is IFormattable formattable)
                return formattable.ToString(null, CultureInfo.InvariantCulture);
            return val.ToString();
        }
        public string AsSubstitute()
        {
            return AsSubstitute(Value);
        }

        public override string ToString()
        {
            return $"{ParameterName}({DbType}): {Value}";
        }
    }
}
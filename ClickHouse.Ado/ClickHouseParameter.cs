using System;
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

        public string AsSubstitute()
        {
            if (DbType == DbType.String
#if !NETCOREAPP11
                || DbType == DbType.AnsiString || DbType == DbType.StringFixedLength || DbType == DbType.AnsiStringFixedLength
#endif
                )
                return ProtocolFormatter.EscapeStringValue(Value.ToString());
            if (DbType == DbType.DateTime
#if !NETCOREAPP11
                || DbType == DbType.DateTime2 || DbType == DbType.DateTime2
#endif
                )
                return $"'{(DateTime)Value:yyyy-MM-dd HH:mm:ss}'";
            if (DbType == DbType.Date)
                return $"'{(DateTime)Value:yyyy-MM-dd}'";
            return Value.ToString();
        }
    }
}
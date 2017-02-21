using System;
using System.Data;
using ClickHouse.Ado.Impl;

namespace ClickHouse.Ado
{
    public class ClickHouseParameter : IDbDataParameter
    {
        public DbType DbType { get; set; }
        ParameterDirection IDataParameter.Direction { get; set; }
        bool IDataParameter.IsNullable => false;
        public string ParameterName { get; set; }
        string IDataParameter.SourceColumn { get; set; }
        DataRowVersion IDataParameter.SourceVersion { get; set; }
        public object Value { get; set; }
        byte IDbDataParameter.Precision { get; set; }
        byte IDbDataParameter.Scale { get; set; }
        int IDbDataParameter.Size { get; set; }

        public string AsSubstitute()
        {
            if (DbType == DbType.String || DbType == DbType.AnsiString || DbType == DbType.StringFixedLength || DbType == DbType.AnsiStringFixedLength)
                return ProtocolFormatter.EscapeStringValue(Value.ToString());
            if (DbType == DbType.DateTime || DbType == DbType.DateTime2 || DbType == DbType.DateTime2)
                return $"'{(DateTime)Value:yyyy-MM-dd HH:mm:ss}'";
            if (DbType == DbType.Date)
                return $"'{(DateTime)Value:yyyy-MM-dd}'";
            return Value.ToString();
        }
    }
}
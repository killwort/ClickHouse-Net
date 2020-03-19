using System;
#if !NETCOREAPP11
using System.Data;

#endif

namespace ClickHouse.Ado {
    public static class AdoExtensions {
        public static void ReadAll<T>(this T reader, Action<T> rowAction) where T :
#if NETCOREAPP11
            ClickHouseDataReader
#else
            IDataReader
#endif
        {
            do {
                while (reader.Read()) rowAction(reader);
            } while (reader.NextResult());
        }

#if NETCOREAPP11
        public static ClickHouseCommand AddParameter(this ClickHouseCommand cmd, string name, DbType type, object value)
#else
        public static T AddParameter<T>(this T cmd, string name, DbType type, object value) where T : IDbCommand
#endif
        {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.DbType = type;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }
#if NETCOREAPP11
        public static ClickHouseCommand AddParameter(this ClickHouseCommand cmd, string name, object value)
#else
        public static T AddParameter<T>(this T cmd, string name, object value) where T : IDbCommand
#endif
        {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }
    }
}
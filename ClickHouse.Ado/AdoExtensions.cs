using System;
using System.Data;

namespace ClickHouse.Ado {
    public static class AdoExtensions {
        public static void ReadAll<T>(this T reader, Action<T> rowAction) where T : IDataReader {
            do {
                while (reader.Read()) rowAction(reader);
            } while (reader.NextResult());
        }

        public static T AddParameter<T>(this T cmd, string name, DbType type, object value) where T : IDbCommand {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.DbType = type;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }

        public static T AddParameter<T>(this T cmd, string name, object value) where T : IDbCommand {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }
    }
}
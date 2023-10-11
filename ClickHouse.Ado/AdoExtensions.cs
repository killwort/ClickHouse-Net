using System;
using System.Data;

namespace ClickHouse.Ado;

/// <summary>
///     Some extensions to make life with ADO easier.
/// </summary>
public static class AdoExtensions {
    /// <summary>
    ///     Read all rows from all result sets from the <see cref="IDataReader" />.
    /// </summary>
    /// <param name="reader">Data reader.</param>
    /// <param name="rowAction">Action to call for each row.</param>
    /// <typeparam name="T"><see cref="IDataReader" /> implementing class.</typeparam>
    public static void ReadAll<T>(this T reader, Action<T> rowAction) where T : IDataReader {
        do {
            while (reader.Read()) rowAction(reader);
        } while (reader.NextResult());
    }

    /// <summary>
    ///     Adds named parameter with type and value to the command (chainable).
    /// </summary>
    /// <param name="cmd">Command.</param>
    /// <param name="name">Parameter name.</param>
    /// <param name="type">Parameter type.</param>
    /// <param name="value">Parameter value.</param>
    /// <typeparam name="T">Type of <see cref="IDbCommand" />.</typeparam>
    /// <returns><code>this</code> for call chaining.</returns>
    public static T AddParameter<T>(this T cmd, string name, DbType type, object value) where T : IDbCommand {
        var par = cmd.CreateParameter();
        par.ParameterName = name;
        par.DbType = type;
        par.Value = value;
        cmd.Parameters.Add(par);
        return cmd;
    }

    /// <summary>
    ///     Adds named parameter with type and value to the command (chainable).
    /// </summary>
    /// <param name="cmd">Command.</param>
    /// <param name="name">Parameter name.</param>
    /// <param name="value">Parameter value.</param>
    /// <typeparam name="T">Type of <see cref="IDbCommand" />.</typeparam>
    /// <returns><code>this</code> for call chaining.</returns>
    public static T AddParameter<T>(this T cmd, string name, object value) where T : IDbCommand {
        var par = cmd.CreateParameter();
        par.ParameterName = name;
        par.Value = value;
        cmd.Parameters.Add(par);
        return cmd;
    }
}
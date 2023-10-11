using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ClickHouse.Ado;

/// <summary>
///     Clickhouse connection settings.
/// </summary>
/// <remarks>
///     Most connection settings are ripped from clickhouse server and native client sources and completely ignored by this
///     driver.
///     See docs on individual setting to check if it should work or not.
/// </remarks>
public class ClickHouseConnectionSettings {
    private static readonly Dictionary<string, PropertyInfo> Properties;

    static ClickHouseConnectionSettings() {
#if CORE_FRAMEWORK
        Properties = typeof(ClickHouseConnectionSettings).GetTypeInfo().GetProperties().ToDictionary(x => x.Name, x => x);
#else
        Properties = typeof(ClickHouseConnectionSettings).GetProperties().ToDictionary(x => x.Name, x => x);
#endif
    }

    /// <summary>
    ///     Creates empty connection settings.
    /// </summary>
    public ClickHouseConnectionSettings() { }

    /// <summary>
    ///     Creates connection settings from the connection string.
    /// </summary>
    /// <param name="connectionString">Connection string.</param>
    /// <exception cref="FormatException">When connection string parameter is not supported.</exception>
    public ClickHouseConnectionSettings(string connectionString) {
        var varName = new StringBuilder();
        var varValue = new StringBuilder();

        char? valueEscape = null;
        var inEscape = false;
        var inValue = false;
        foreach (var c in connectionString)
            if (inEscape) {
                if (inValue) varValue.Append(c);
                else varName.Append(c);
                inEscape = false;
            } else if (valueEscape.HasValue) {
                if (valueEscape.Value == c) {
                    valueEscape = null;
                } else {
                    if (inValue) varValue.Append(c);
                    else varName.Append(c);
                }
            } else if (c == '\\') {
                inEscape = true;
            } else if (c == '"' || c == '\'') {
                valueEscape = c;
            } else if (char.IsWhiteSpace(c)) {
            } else if (c == '=') {
                if (inValue) throw new FormatException($"Value for parameter {varName} in the connection string contains unescaped '='.");
                inValue = true;
            } else if (c == ';') {
                if (!inValue) throw new FormatException($"No value for parameter {varName} in the connection string.");
                SetValue(varName.ToString(), varValue.ToString());
                inValue = false;
                varName.Clear();
                varValue.Clear();
            } else {
                if (inValue) varValue.Append(c);
                else varName.Append(c);
            }

        if (inValue) SetValue(varName.ToString(), varValue.ToString());
    }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public bool Async { get; set; }

    /// <summary>
    ///     Size of socket buffer in bytes. Should be at least 4K, and 32K+ improves stability on slower networks and long
    ///     requests.
    /// </summary>
    public int BufferSize { get; set; } = 4096;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int ApacheBufferSize { get; set; }

    /// <summary>
    ///     Timeout for low-level socket operations in milliseconds. Default 1000 (1s) should be reasonable for most cases.
    /// </summary>
    public int SocketTimeout { get; set; } = 1000;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int ConnectionTimeout { get; set; } = 1000;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int DataTransferTimeout { get; set; } = 1000;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int KeepAliveTimeout { get; set; } = 1000;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int TimeToLiveMillis { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int DefaultMaxPerRoute { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxTotal { get; set; }

    /// <summary>
    ///     Name of the client (it is logged on server).
    /// </summary>
    public string ClientName { get; set; }

    /// <summary>
    ///     Hostname of the server. Or IP address.
    /// </summary>
    public string Host { get; set; }

    /// <summary>
    ///     Port number to connect to.
    /// </summary>
    /// <remarks>
    ///     It should be native protocol port, not HTTP.
    ///     By default clickhouse uses port 9000 in non-TLS mode and 9440 in TLS (see <see cref="Encrypt" />).
    /// </remarks>
    public int Port { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxCompressBufferSize { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxParallelReplicas { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public string TotalsMode { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public string QuotaKey { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    ///     Name of the database to connect to. Can be changed later by SQL "USE" or
    ///     <see cref="ClickHouseConnection.ChangeDatabase" />.
    /// </summary>
    public string Database { get; set; }

    /// <summary>
    ///     Use data compression. Drastically reduces network traffic in some cases and mostly useless if you do OLAP queries
    ///     resulting in &lt;100 rows.
    /// </summary>
    public bool Compress { get; set; }

    /// <summary>
    ///     Which compressor to use. Should be set to default LZ4, as zstd and LZ4HC are not supported by this driver.
    /// </summary>

    public string Compressor { get; set; }

    /// <summary>
    ///     Check hashes of compressed data. Should be left on (default).
    /// </summary>
    public bool CheckCompressedHash { get; set; } = true;

    /// <summary>
    ///     Ignored.
    /// </summary>
    public bool Decompress { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public bool Extremes { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxThreads { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxExecutionTime { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxBlockSize { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxRowsToGroupBy { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public string Profile { get; set; }

    /// <summary>
    ///     User name to connect with.
    /// </summary>
    public string User { get; set; }

    /// <summary>
    ///     Password to connect with.
    /// </summary>
    public string Password { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public bool DistributedAggregationMemoryEfficient { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxBytesBeforeExternalGroupBy { get; set; }

    /// <summary>
    ///     Ignored.
    /// </summary>
    public int MaxBytesBeforeExternalSort { get; set; }

    /// <summary>
    ///     Wrap all the network traffic into TLS stream. Also enables to use
    ///     <see cref="ClickHouseConnection.TlsServerCertificateValidationCallback" /> and
    ///     <see cref="ClickHouseConnection.TlsClientCertificate" />.
    /// </summary>
    public bool Encrypt { get; set; } = false;

    /// <summary>
    ///     Write all the queries (in their final form) to the <see cref="System.Diagnostics.Trace" /> with "ClickHouse.Ado"
    ///     category.
    /// </summary>
    /// <remarks>All parameter values (except for bulk inserts) will be present in the trace output! Beware security risks!</remarks>
    public bool Trace { get; set; } = false;

    private void SetValue(string name, string value) {
#if CLASSIC_FRAMEWORK
        Properties[name].GetSetMethod()
#else
        Properties[name].SetMethod
#endif
                        .Invoke(this, new[] { Convert.ChangeType(value, Properties[name].PropertyType) });
    }

    /// <summary>
    ///     Converts connection settings back to connection string form.
    /// </summary>
    /// <returns>Connection string.</returns>
    public override string ToString() {
        var builder = new StringBuilder();
        foreach (var prop in Properties) {
            var value = prop.Value.GetValue(this, null);
            if (value == null) continue;

            builder.Append(prop.Key);
            builder.Append("=\"");
            builder.Append(value.ToString().Replace("\\", "\\\\").Replace("\"", "\\\""));
            builder.Append("\";");
        }

        return builder.ToString();
    }
}
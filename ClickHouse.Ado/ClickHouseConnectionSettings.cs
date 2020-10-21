using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ClickHouse.Ado {
    public class ClickHouseConnectionSettings {
        private static readonly Dictionary<string, PropertyInfo> Properties;

        static ClickHouseConnectionSettings() {
#if CORE_FRAMEWORK
            Properties = typeof(ClickHouseConnectionSettings).GetTypeInfo().GetProperties().ToDictionary(x => x.Name, x => x);
#else
			Properties = typeof(ClickHouseConnectionSettings).GetProperties().ToDictionary(x => x.Name, x => x);
#endif
        }

        public ClickHouseConnectionSettings() { }

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

        public bool Async { get; set; }
        public int BufferSize { get; set; } = 4096;
        public int ApacheBufferSize { get; set; }
        public int SocketTimeout { get; set; } = 1000;
        public int ConnectionTimeout { get; set; } = 1000;
        public int DataTransferTimeout { get; set; } = 1000;
        public int KeepAliveTimeout { get; set; } = 1000;
        public int TimeToLiveMillis { get; set; }
        public int DefaultMaxPerRoute { get; set; }
        public int MaxTotal { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }

        //additional
        public int MaxCompressBufferSize { get; set; }

        // queries settings
        public int MaxParallelReplicas { get; set; }
        public string TotalsMode { get; set; }
        public string QuotaKey { get; set; }
        public int Priority { get; set; }
        public string Database { get; set; }
        public bool Compress { get; set; }
        public string Compressor { get; set; }
        public bool CheckCompressedHash { get; set; } = true;
        public bool Decompress { get; set; }
        public bool Extremes { get; set; }
        public int MaxThreads { get; set; }
        public int MaxExecutionTime { get; set; }
        public int MaxBlockSize { get; set; }
        public int MaxRowsToGroupBy { get; set; }
        public string Profile { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public bool DistributedAggregationMemoryEfficient { get; set; }
        public int MaxBytesBeforeExternalGroupBy { get; set; }
        public int MaxBytesBeforeExternalSort { get; set; }

        private void SetValue(string name, string value) {
#if CLASSIC_FRAMEWORK
            Properties[name].GetSetMethod()
#else
            Properties[name].SetMethod
#endif
                            .Invoke(this, new[] {Convert.ChangeType(value, Properties[name].PropertyType)});
        }

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
}
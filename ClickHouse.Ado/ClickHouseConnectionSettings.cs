using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ClickHouse.Ado
{
    public class ClickHouseConnectionSettings
    {
        private static readonly Dictionary<string, PropertyInfo> Properties;

        static ClickHouseConnectionSettings()
        {
#if NETSTANDARD15
			Properties = typeof(ClickHouseConnectionSettings).GetTypeInfo().GetProperties().ToDictionary(x => x.Name, x => x);
#else
			Properties = typeof(ClickHouseConnectionSettings).GetProperties().ToDictionary(x => x.Name, x => x);
#endif
		}
        private void SetValue(string name, string value)
        {
#if FRAMEWORK20 || FRAMEWORK40
            Properties[name].GetSetMethod()
#else
            Properties[name].SetMethod
#endif
            .Invoke(this, new[] {Convert.ChangeType(value, Properties[name].PropertyType)});
        }
        public ClickHouseConnectionSettings(string connectionString)
        {
            StringBuilder varName = new StringBuilder();
            StringBuilder varValue = new StringBuilder();
            
            char? valueEscape=null;
            bool inEscape=false;
            bool inValue = false;
            foreach (char c in connectionString)
            {
                if (inEscape)
                {
                    if (inValue) varValue.Append(c);
                    else varName.Append(c);
                    inEscape = false;
                }
                else if (valueEscape.HasValue)
                {
                    if (valueEscape.Value == c)
                        valueEscape = null;
                    else
                    {
                        if (inValue) varValue.Append(c);
                        else varName.Append(c);
                    }
                }
                else if (c == '\\')
                    inEscape = true;
                else if ( c == '"' || c == '\'')
                
                    valueEscape = c;
                else if (char.IsWhiteSpace(c))
                    continue;
                else if (c == '=')
                {
                    if(inValue)throw new FormatException($"Value for parameter {varName} in the connection string contains unescaped '='.");
                    inValue = true;
                }else if (c == ';')
                {
                    if(!inValue)throw new FormatException($"No value for parameter {varName} in the connection string.");
                    SetValue(varName.ToString(),varValue.ToString());
                    inValue = false;
                    varName.Clear();
                    varValue.Clear();
                }
                else
                {
                    if (inValue) varValue.Append(c);
                    else varName.Append(c);
                }
            }
            if (inValue) SetValue(varName.ToString(), varValue.ToString());
        }

        public bool Async{ get; private set; }
        public int BufferSize { get; private set; } = 4096;
        public int ApacheBufferSize{ get; private set; }
        public int SocketTimeout { get; private set; } = 1000;
        public int ConnectionTimeout { get; private set; } = 1000;
        public int DataTransferTimeout { get; private set; } = 1000;
        public int KeepAliveTimeout{ get; private set; } = 1000;
        public int TimeToLiveMillis { get; private set; }
        public int DefaultMaxPerRoute{ get; private set; }
        public int MaxTotal{ get; private set; }
        public string Host{ get; private set; }
        public int Port{ get; private set; }

        //additional
        public int MaxCompressBufferSize{ get; private set; }


        // queries settings
        public int MaxParallelReplicas{ get; private set; }
        public string TotalsMode{ get; private set; }
        public string QuotaKey{ get; private set; }
        public int Priority{ get; private set; }
        public string Database{ get; private set; }
        public bool Compress { get; private set; }
        public string Compressor { get; private set; }
        public bool CheckCompressedHash { get; private set; } = true;
        public bool Decompress{ get; private set; }
        public bool Extremes{ get; private set; }
        public int MaxThreads{ get; private set; }
        public int MaxExecutionTime{ get; private set; }
        public int MaxBlockSize{ get; private set; }
        public int MaxRowsToGroupBy{ get; private set; }
        public string Profile{ get; private set; }
        public string User{ get; private set; }
        public string Password{ get; private set; }
        public bool DistributedAggregationMemoryEfficient{ get; private set; }
        public int MaxBytesBeforeExternalGroupBy{ get; private set; }
        public int MaxBytesBeforeExternalSort{ get; private set; }

        public override string ToString()
        {
            var builder = new StringBuilder();
            foreach (var prop in Properties)
            {
                var value = prop.Value.GetValue(this, null);
                if (value == null)
                {
                    continue;
                }

                builder.Append(prop.Key);
                builder.Append("=\"");
                builder.Append(value.ToString().Replace("\\", "\\\\").Replace("\"", "\\\""));
                builder.Append("\";");
            }
            return builder.ToString();
        }
    }
}
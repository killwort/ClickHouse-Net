using System;
using System.Net;

namespace ClickHouse.Ado.Impl.Data {
    internal class ClientInfo {
        public ClientInfo() {
            QueryKind = QueryKind.Initial;
            Interface = Interface.Tcp;
            HttpMethod = HttpMethod.Unknown;
            ClientName = ProtocolCaps.ClientName;
            ClientVersionMajor = 1;
            ClientVersionMinor = 1;
            ClientRevision = 54411;
        }

        public QueryKind QueryKind { get; set; }

        public string CurrentUser { get; set; }
        public string CurrentQueryId { get; set; }
        public EndPoint CurrentAddress { get; set; }
        public string InitialUser { get; set; }
        public string InitialQueryId { get; set; }
        public EndPoint InitialAddress { get; set; }
        public Interface Interface { get; set; }
        public string OsUser { get; set; }
        public string ClientHostname { get; set; }
        public string ClientName { get; set; }
        public long ClientVersionMajor { get; }
        public long ClientVersionMinor { get; }
        public uint ClientRevision { get; }
        public HttpMethod HttpMethod { get; set; }
        public string HttpUserAgent { get; set; }
        public string QuotaKey { get; set; }

        internal void Write(ProtocolFormatter formatter) {
            formatter.WriteByte((byte) QueryKind);
            if (QueryKind == QueryKind.None) return;
            formatter.WriteString(InitialUser);
            formatter.WriteString(InitialQueryId);
            formatter.WriteString(InitialAddress?.ToString());
            formatter.WriteByte((byte) Interface);
            switch (Interface) {
                case Interface.Tcp:
                    formatter.WriteString(OsUser);
                    formatter.WriteString(ClientHostname);
                    formatter.WriteString(ClientName);
                    formatter.WriteUInt(ClientVersionMajor);
                    formatter.WriteUInt(ClientVersionMinor);
                    formatter.WriteUInt(ClientRevision);
                    break;
                case Interface.Http:
                    formatter.WriteByte((byte) HttpMethod);
                    formatter.WriteString(HttpUserAgent);
                    break;
            }

            if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithQuotaKeyInClientInfo)
                formatter.WriteString(QuotaKey);
            if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithServerVersionPatch)
                formatter.WriteUInt(ClientRevision);
        }

        public void PopulateEnvironment() {
#if CLASSIC_FRAMEWORK
			OsUser = Environment.UserName;
#endif
            ClientHostname = Environment.MachineName;
        }
    }
}

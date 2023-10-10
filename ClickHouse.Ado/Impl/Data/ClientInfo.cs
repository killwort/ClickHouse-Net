using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data; 

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

    internal async Task Write(ProtocolFormatter formatter, string clientName, CancellationToken cToken) {
        await formatter.WriteByte((byte)QueryKind, cToken);
        if (QueryKind == QueryKind.None) return;
        await formatter.WriteString(InitialUser, cToken);
        await formatter.WriteString(InitialQueryId, cToken);
        await formatter.WriteString(InitialAddress?.ToString(), cToken);
        await formatter.WriteByte((byte)Interface, cToken);
        switch (Interface) {
            case Interface.Tcp:
                await formatter.WriteString(OsUser, cToken);
                await formatter.WriteString(ClientHostname, cToken);
                await formatter.WriteString(string.IsNullOrWhiteSpace(clientName) ? ClientName : clientName, cToken);
                await formatter.WriteUInt(ClientVersionMajor, cToken);
                await formatter.WriteUInt(ClientVersionMinor, cToken);
                await formatter.WriteUInt(ClientRevision, cToken);
                break;
            case Interface.Http:
                await formatter.WriteByte((byte)HttpMethod, cToken);
                await formatter.WriteString(HttpUserAgent, cToken);
                break;
        }

        if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithQuotaKeyInClientInfo)
            await formatter.WriteString(QuotaKey, cToken);
        if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithServerVersionPatch)
            await formatter.WriteUInt(ClientRevision, cToken);
    }

    public void PopulateEnvironment() => ClientHostname = Environment.MachineName;
}
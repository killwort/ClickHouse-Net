using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado;

public class ClickHouseConnection : DbConnection, IDbConnection {
    private Stream _connectionStream;

    private string _database;

    private bool _isBroken;

    private TcpClient _tcpClient;

    internal SemaphoreSlim DialogueLock = new(1, 1);

    public ClickHouseConnection() { }

    public ClickHouseConnection(ClickHouseConnectionSettings settings) => ConnectionSettings = settings;

    public ClickHouseConnection(string connectionString) => ConnectionSettings = new ClickHouseConnectionSettings(connectionString);

    public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

    internal ProtocolFormatter Formatter { get; set; }

    public X509Certificate2 TlsClientCertificate { get; set; }
    public RemoteCertificateValidationCallback TlsServerCertificateValidationCallback { get; set; } = (_, _, _, _) => true;

    public ServerInfo ServerInfo => Formatter.ServerInfo;
    public override string DataSource { get; }

    public override string ServerVersion { get; }

    public void Dispose() {
        if (_tcpClient != null) Close();
    }

    public override void Close() {
        if (Formatter != null) {
            Formatter.Close();
            Formatter = null;
        }

        if (_connectionStream != null) {
            _connectionStream.Dispose();
            _connectionStream = null;
        }

        if (_tcpClient != null) {
            _tcpClient.Dispose();
            _tcpClient = null;
        }
    }

    /// <inheritdoc />
    public override void Open() => OpenAsync().Wait();

    public override string ConnectionString { get => ConnectionSettings.ToString(); set => ConnectionSettings = new ClickHouseConnectionSettings(value); }

    public int ConnectionTimeout { get; set; } = 10000;
    public override string Database => _database;

    public override void ChangeDatabase(string databaseName) {
        CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
        _database = databaseName;
    }

    public override ConnectionState State => Formatter != null ? _isBroken ? ConnectionState.Broken : ConnectionState.Open : ConnectionState.Closed;

    public IDbTransaction BeginTransaction() => throw new NotSupportedException();

    public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();

    IDbCommand IDbConnection.CreateCommand() => new ClickHouseCommand(this);

    private async Task Connect(TcpClient client, string hostName, int port, CancellationToken cToken) {
        await client.ConnectAsync(hostName, port, cToken);
        cToken.ThrowIfCancellationRequested();
        if (!client.Connected)
            throw new InvalidOperationException("Connection failure");
    }

    /// <inheritdoc />
    public override async Task OpenAsync(CancellationToken cToken) {
        await DialogueLock.WaitAsync(cToken);
        try {
            if (_tcpClient != null) throw new InvalidOperationException("Connection already open.");
            _tcpClient = new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
            await Connect(_tcpClient, ConnectionSettings.Host, ConnectionSettings.Port, cToken);
            var netStream = new NetworkStream(_tcpClient.Client);
            if (ConnectionSettings.Encrypt) {
                var sslStream = new SslStream(netStream, true, TlsServerCertificateValidationCallback);
                var authOptions = new SslClientAuthenticationOptions();
                authOptions.TargetHost = ConnectionSettings.Host;
                if (TlsClientCertificate != null)
                    (authOptions.ClientCertificates ??= new X509CertificateCollection()).Add(TlsClientCertificate);
                await sslStream.AuthenticateAsClientAsync(authOptions, cToken);
                _connectionStream = sslStream;
            } else {
                _connectionStream = netStream;
            }

            var ci = new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(this, _connectionStream, ci, ConnectionSettings.SocketTimeout);
            await Formatter.Handshake(ConnectionSettings, cToken);
            _database = ConnectionSettings.Database;
        } finally {
            DialogueLock.Release();
        }
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
    protected override DbCommand CreateDbCommand() => new ClickHouseCommand(this);

    public ClickHouseCommand CreateCommand() => new(this);

    public ClickHouseCommand CreateCommand(string text) => new(this, text);

    internal void MaybeSetBroken(Exception exception) {
        if (exception is ClickHouseException) return;
        _isBroken = true;
    }
}

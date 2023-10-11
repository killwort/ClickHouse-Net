using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado;

/// <summary>
///     Clickhouse-specific database connection.
/// </summary>
public class ClickHouseConnection : DbConnection, IDbConnection {
    internal readonly SemaphoreSlim DialogueLock = new(1, 1);
    private Stream _connectionStream;

    private string _database;

    private bool _isBroken;

    private TcpClient _tcpClient;

    /// <summary>
    ///     Creates empty connection.
    /// </summary>
    public ClickHouseConnection() { }

    /// <summary>
    ///     Creates connection with specified connection settings.
    /// </summary>
    /// <param name="settings">Connection settings.</param>
    public ClickHouseConnection(ClickHouseConnectionSettings settings) => ConnectionSettings = settings;

    /// <summary>
    ///     Creates connection with connection string.
    /// </summary>
    /// <param name="connectionString">Connection string.</param>
    public ClickHouseConnection(string connectionString) => ConnectionSettings = new ClickHouseConnectionSettings(connectionString);

    /// <summary>
    ///     Connection settings read from connection string.
    /// </summary>
    public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

    internal ProtocolFormatter Formatter { get; set; }

    /// <summary>
    ///     TLS client certificate to use for authentication. <see cref="ClickHouseConnectionSettings.Encrypt" /> should be set
    ///     to <code>true</code>.
    /// </summary>
    public X509Certificate2 TlsClientCertificate { get; set; }

    /// <summary>
    ///     Validation callback to use on server certificate. <see cref="ClickHouseConnectionSettings.Encrypt" /> should be set
    ///     to <code>true</code>.
    /// </summary>
    /// <remarks>By default it accepts all certificates: self-signed, expired, etc.</remarks>
    public RemoteCertificateValidationCallback TlsServerCertificateValidationCallback { get; set; } = (_, _, _, _) => true;

    /// <summary>
    ///     Information about connected server.
    /// </summary>
    public ServerInfo ServerInfo => Formatter.ServerInfo;

    /// <inheritdoc />
    public override string DataSource { get; }

    /// <inheritdoc />
    public override string ServerVersion => $"{ServerInfo?.Major}.{ServerInfo?.Minor}.{ServerInfo?.Build}.{ServerInfo?.Patch}";

    /// <inheritdoc />
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

    /// <inheritdoc />
    public override string ConnectionString { get => ConnectionSettings.ToString(); set => ConnectionSettings = new ClickHouseConnectionSettings(value); }

    /// <inheritdoc />
    public override string Database => _database;

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName) {
        CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
        _database = databaseName;
    }

    /// <inheritdoc />
    public override ConnectionState State => Formatter != null ? _isBroken ? ConnectionState.Broken : ConnectionState.Open : ConnectionState.Closed;

    IDbCommand IDbConnection.CreateCommand() => new ClickHouseCommand(this);

    /// <inheritdoc />
    protected override void Dispose(bool disposing) {
        if (_tcpClient != null) Close();
    }

    private async Task Connect(TcpClient client, string hostName, int port, CancellationToken cToken) {
#if MODERN_CORE_FRAMEWORK
        await client.ConnectAsync(hostName, port, cToken);
#else
        await client.ConnectAsync(hostName, port);
#endif
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
#if CORE_FRAMEWORK
                var authOptions = new SslClientAuthenticationOptions();
                authOptions.TargetHost = ConnectionSettings.Host;
                if (TlsClientCertificate != null)
                    (authOptions.ClientCertificates ??= new X509CertificateCollection()).Add(TlsClientCertificate);
                await sslStream.AuthenticateAsClientAsync(authOptions, cToken);
#else
                if (TlsClientCertificate != null)
                    await sslStream.AuthenticateAsClientAsync(ConnectionSettings.Host, new X509CertificateCollection { TlsClientCertificate }, SslProtocols.Tls12, false);
                else
                    await sslStream.AuthenticateAsClientAsync(ConnectionSettings.Host, new X509CertificateCollection(), SslProtocols.Tls12, false);
#endif
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

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand() => new ClickHouseCommand(this);

    /// <inheritdoc cref="CreateCommand()" />
    public new ClickHouseCommand CreateCommand() => new(this);

    /// <inheritdoc cref="CreateCommand()" />
    public ClickHouseCommand CreateCommand(string text) => new(this, text);

    internal void MaybeSetBroken(Exception exception) {
        if (exception is ClickHouseException) return;
        _isBroken = true;
    }
}
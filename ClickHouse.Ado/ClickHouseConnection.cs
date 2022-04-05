using System;
using System.Data;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado {
    public class ClickHouseConnection : IDbConnection {
        private Stream _connectionStream;

        private TcpClient _tcpClient;

        private bool _isBroken;

        public ClickHouseConnection() { }

        public ClickHouseConnection(ClickHouseConnectionSettings settings) => ConnectionSettings = settings;

        public ClickHouseConnection(string connectionString) => ConnectionSettings = new ClickHouseConnectionSettings(connectionString);

        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

        internal ProtocolFormatter Formatter { get; set; }

        public void Dispose() {
            if (_tcpClient != null) Close();
        }

        public void Close() {
            if (_connectionStream != null) {
#if CLASSIC_FRAMEWORK
                _connectionStream.Close();
#endif
                _connectionStream.Dispose();
                _connectionStream = null;
            }

            if (_tcpClient != null) {
#if CLASSIC_FRAMEWORK
				_tcpClient.Close();
#else
                _tcpClient.Dispose();
#endif
                _tcpClient = null;
            }

            if (Formatter != null) {
                Formatter.Close();
                Formatter = null;
            }
        }



        private void Connect(TcpClient client, string hostName, int port, int timeout) {
#if CORE_FRAMEWORK
            var cTask = client.ConnectAsync(hostName, port);
            if (!cTask.Wait(timeout) || !client.Connected) {
                cTask.ContinueWith(_ => client.Dispose());
                throw new TimeoutException("Timeout waiting for connection.");
            }
#else
            var state = new TcpClientState {
                Client = client,
                Success = true
            };
            var ar = client.BeginConnect(hostName, port, EndConnect, state);
            state.Success = ar.AsyncWaitHandle.WaitOne(timeout, false);
            if (!state.Success || !client.Connected)
                throw new TimeoutException("Timeout waiting for connection.");
}
        private class TcpClientState
        {
            public TcpClient Client { get; set; }
            public bool Success { get; set; }
        }
        private void EndConnect(IAsyncResult ar) {
            var state = (TcpClientState) ar.AsyncState;
            try {
                state.Client.EndConnect(ar);
            } catch {
            }

            var isConnected = state.Client?.Client != null ? state.Client.Connected : false;

            if (isConnected && state.Success)
                return;

            state.Client.Close();

#endif
        }

        public void Open() {
            if (_tcpClient != null) throw new InvalidOperationException("Connection already open.");
            _tcpClient = new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
            Connect(_tcpClient, ConnectionSettings.Host, ConnectionSettings.Port, ConnectionTimeout);
            var netStream = new NetworkStream(_tcpClient.Client);
            if (ConnectionSettings.Encrypt)
            {
                // TODO: Fix with proper certification validation
                var sslStream = new SslStream(netStream, true, new RemoteCertificateValidationCallback((_1, _2, _3, _4) => true));
                sslStream.AuthenticateAsClient(ConnectionSettings.Host);
                _connectionStream = sslStream;
            }
            else _connectionStream = netStream;
            var ci = new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(this, _connectionStream, ci, () => _tcpClient.Client.Poll(ConnectionSettings.SocketTimeout, SelectMode.SelectRead), ConnectionSettings.SocketTimeout);
            Formatter.Handshake(ConnectionSettings);
        }

        public ServerInfo ServerInfo => Formatter.ServerInfo;

        public string ConnectionString { get => ConnectionSettings.ToString(); set => ConnectionSettings = new ClickHouseConnectionSettings(value); }

        public int ConnectionTimeout { get; set; } = 10000;
        public string Database { get; private set; }

        public void ChangeDatabase(string databaseName) {
            CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
            Database = databaseName;
        }

        public ConnectionState State => Formatter != null ? _isBroken ? ConnectionState.Broken : ConnectionState.Open : ConnectionState.Closed;

        public IDbTransaction BeginTransaction() => throw new NotSupportedException();

        public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();

        IDbCommand IDbConnection.CreateCommand() => new ClickHouseCommand(this);

        public ClickHouseCommand CreateCommand() => new ClickHouseCommand(this);

        public ClickHouseCommand CreateCommand(string text) => new ClickHouseCommand(this, text);

        internal void MaybeSetBroken(Exception exception)
        {
            if (exception is ClickHouseException) return;
            _isBroken = true;
        }
    }
}

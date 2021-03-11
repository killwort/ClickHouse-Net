using System;
using System.Data;
using System.IO;
using System.Net.Sockets;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado {
    public class ClickHouseConnection : IDbConnection {
        private NetworkStream _netStream;
        private Stream _stream;

        private TcpClient _tcpClient;

        public ClickHouseConnection() { }

        public ClickHouseConnection(ClickHouseConnectionSettings settings) => ConnectionSettings = settings;

        public ClickHouseConnection(string connectionString) => ConnectionSettings = new ClickHouseConnectionSettings(connectionString);

        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

        /*private BinaryReader _reader;
        private BinaryWriter _writer;*/
        internal ProtocolFormatter Formatter { get; set; }

        public void Dispose() {
            if (_tcpClient != null) Close();
        }

        public void Close() {
            /*if (_reader != null)
            {
                _reader.Close();
                _reader.Dispose();
                _reader = null;
            }
            if (_writer != null)
            {
                _writer.Close();
                _writer.Dispose();
                _writer = null;
            }*/
            if (_stream != null) {
#if CLASSIC_FRAMEWORK
				_stream.Close();
#endif
                _stream.Dispose();
                _stream = null;
            }

            if (_netStream != null) {
#if CLASSIC_FRAMEWORK
				_netStream.Close();
#endif
                _netStream.Dispose();
                _netStream = null;
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

            if (state.Client.Connected && state.Success)
                return;

            state.Client.Close();

#endif
        }

        public void Open() {
            if (_tcpClient != null) throw new InvalidOperationException("Connection already open.");
            _tcpClient = new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            //_tcpClient.NoDelay = true;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
            Connect(_tcpClient, ConnectionSettings.Host, ConnectionSettings.Port, ConnectionTimeout);
            _netStream = new NetworkStream(_tcpClient.Client);
            _stream = new UnclosableStream(_netStream);
            /*_reader=new BinaryReader(new UnclosableStream(_stream));
            _writer=new BinaryWriter(new UnclosableStream(_stream));*/
            var ci = new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(_stream, ci, () => _tcpClient.Client.Poll(ConnectionSettings.SocketTimeout, SelectMode.SelectRead), ConnectionSettings.SocketTimeout);
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

        public ConnectionState State => Formatter != null ? ConnectionState.Open : ConnectionState.Closed;

        public IDbTransaction BeginTransaction() => throw new NotSupportedException();

        public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();

        IDbCommand IDbConnection.CreateCommand() => new ClickHouseCommand(this);

        public ClickHouseCommand CreateCommand() => new ClickHouseCommand(this);

        public ClickHouseCommand CreateCommand(string text) => new ClickHouseCommand(this, text);
    }
}

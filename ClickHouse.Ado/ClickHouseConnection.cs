using System;
#if !NETCOREAPP11
using System.Data;
#endif
using System.IO;
using System.Net.Sockets;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado
{
    public class ClickHouseConnection
#if !NETCOREAPP11
        : IDbConnection
#endif
    {
        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

        public ClickHouseConnection()
        {
        }

        public ClickHouseConnection(ClickHouseConnectionSettings settings)
        {
            ConnectionSettings = settings;
        }
        public ClickHouseConnection(string connectionString)
        {
            ConnectionSettings = new ClickHouseConnectionSettings(connectionString);
        }

        private TcpClient _tcpClient;
        private Stream _stream;
        /*private BinaryReader _reader;
        private BinaryWriter _writer;*/
        internal ProtocolFormatter Formatter { get;
            set; }
        private NetworkStream _netStream;

        public void Dispose()
        {
            if (_tcpClient != null) Close();
        }

        public void Close()
        {
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
            if (_stream != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_stream.Close();
#endif
				_stream.Dispose();
                _stream = null;
            }
            if (_netStream != null)
            {
#if !NETSTANDARD15 &&!NETCOREAPP11
				_netStream.Close();
#endif
				_netStream.Dispose();
                _netStream = null;
            }
            if (_tcpClient != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_tcpClient.Close();
#else
				_tcpClient.Dispose();
#endif
				_tcpClient = null;
            }
            if (Formatter != null)
            {
                Formatter.Close();
                Formatter = null;
            }
        }


        public void Open()
        {
            if(_tcpClient!=null)throw new InvalidOperationException("Connection already open.");
            _tcpClient=new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            //_tcpClient.NoDelay = true;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
#if NETCOREAPP11
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).Wait();
#elif NETSTANDARD15
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).ConfigureAwait(false).GetAwaiter().GetResult();
#else
			_tcpClient.Connect(ConnectionSettings.Host, ConnectionSettings.Port);
#endif
            _netStream = new NetworkStream(_tcpClient.Client);
            _stream =new UnclosableStream(_netStream);
            /*_reader=new BinaryReader(new UnclosableStream(_stream));
            _writer=new BinaryWriter(new UnclosableStream(_stream));*/
            var ci=new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(_stream,ci, ()=>_tcpClient.Client.Poll(ConnectionSettings.SocketTimeout, SelectMode.SelectRead), 
                ConnectionSettings.SocketTimeout);
            Formatter.Handshake(ConnectionSettings);
        }

        public string ConnectionString
        {
            get { return ConnectionSettings.ToString(); }
            set { ConnectionSettings=new ClickHouseConnectionSettings(value);}
        }

        public int ConnectionTimeout { get; set; }
        public string Database { get; private set; }
#if !NETCOREAPP11
        public ConnectionState State => Formatter != null ? ConnectionState.Open : ConnectionState.Closed;

        public IDbTransaction BeginTransaction()
        {
            throw new NotSupportedException();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }
        IDbCommand IDbConnection.CreateCommand()
        {
            return new ClickHouseCommand(this);
        }
#endif
        public void ChangeDatabase(string databaseName)
        {
            CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
            Database=databaseName;
        }

        public ClickHouseCommand CreateCommand()
        {
            return new ClickHouseCommand(this);
        }
        public ClickHouseCommand CreateCommand(string text)
        {
            return new ClickHouseCommand(this,text);
        }
    }
}

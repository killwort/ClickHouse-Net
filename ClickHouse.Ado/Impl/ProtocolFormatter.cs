using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.Compress;
using ClickHouse.Ado.Impl.Data;
using ClickHouse.Ado.Impl.Settings;

namespace ClickHouse.Ado.Impl;

internal class ProtocolFormatter {
    private static readonly Regex NameRegex = new("^[a-zA-Z_][0-9a-zA-Z_]*$", RegexOptions.Compiled);

    /// <summary>
    ///     Underlaying stream, usually NetworkStream.
    /// </summary>
    private readonly Stream _baseStream;

    private readonly ClickHouseConnection _owner;

    private readonly int _socketTimeout;

    private Compressor _compressor;

    /// <summary>
    ///     Compressed stream, !=null indicated that compression/decompression has beed started.
    /// </summary>
    private Stream _compStream;

    private ClickHouseConnectionSettings _connectionSettings;

    /// <summary>
    ///     Stream to write to/read from, either _baseStream or _compStream.
    /// </summary>
    private Stream _ioStream;

    internal ProtocolFormatter(ClickHouseConnection owner, Stream baseStream, ClientInfo clientInfo, int socketTimeout) {
        _owner = owner;
        _baseStream = baseStream;
        _ioStream = baseStream;
        _socketTimeout = socketTimeout;
        ClientInfo = clientInfo;
    }

    public ServerInfo ServerInfo { get; set; }
    public ClientInfo ClientInfo { get; }

    public async Task Handshake(ClickHouseConnectionSettings connectionSettings, CancellationToken cToken) {
        _connectionSettings = connectionSettings;
        _compressor = connectionSettings.Compress ? Compressor.Create(connectionSettings) : null;
        await WriteUInt((int)ClientMessageType.Hello, cToken);

        await WriteString(string.IsNullOrEmpty(connectionSettings.ClientName) ? ClientInfo.ClientName : connectionSettings.ClientName, cToken);
        await WriteUInt(ClientInfo.ClientVersionMajor, cToken);
        await WriteUInt(ClientInfo.ClientVersionMinor, cToken);
        await WriteUInt(ClientInfo.ClientRevision, cToken);
        await WriteString(connectionSettings.Database, cToken);
        await WriteString(connectionSettings.User, cToken);
        await WriteString(connectionSettings.Password, cToken);
        await _ioStream.FlushAsync(cToken);

        var serverHello = await ReadUInt(cToken);
        if (serverHello == (int)ServerMessageType.Hello) {
            var serverName = await ReadString(cToken);
            var serverMajor = await ReadUInt(cToken);
            var serverMinor = await ReadUInt(cToken);
            var serverBuild = await ReadUInt(cToken);
            string serverTz = null, serverDn = null;
            ulong serverPatch = 0;
            if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerTimezone)
                serverTz = await ReadString(cToken);
            if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerDisplayName)
                serverDn = await ReadString(cToken);
            if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerVersionPatch)
                serverPatch = (uint)await ReadUInt(cToken);
            ServerInfo = new ServerInfo {
                Build = serverBuild,
                Major = serverMajor,
                Minor = serverMinor,
                Name = serverName,
                Timezone = serverTz,
                Patch = (long)serverPatch,
                DisplayName = serverDn
            };
        } else if (serverHello == (int)ServerMessageType.Exception) {
            _owner.MaybeSetBroken(null);
            var cts = new CancellationTokenSource(_socketTimeout);
            await ReadAndThrowException(cts.Token);
        } else {
            _owner.MaybeSetBroken(null);
            throw new FormatException($"Bad message type {serverHello:X} received from server.");
        }
    }

    internal async Task RunQuery(string sql,
                                 QueryProcessingStage stage,
                                 QuerySettings settings,
                                 ClientInfo clientInfo,
                                 IEnumerable<Block> xtables,
                                 bool noData,
                                 CancellationToken cToken) {
        try {
            if (_connectionSettings.Trace)
                Trace.WriteLine($"Executing sql \"{sql}\"", "ClickHouse.Ado");
            await WriteUInt((int)ClientMessageType.Query, cToken);
            await WriteString("", cToken);
            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithClientInfo) {
                if (clientInfo == null)
                    clientInfo = ClientInfo;
                else
                    clientInfo.QueryKind = QueryKind.Secondary;

                await clientInfo.Write(this, _connectionSettings.ClientName, cToken);
            }

            var compressionMethod = _compressor != null ? _compressor.Method : CompressionMethod.Lz4;
            if (settings != null) {
                await settings.Write(this, cToken);
                compressionMethod = settings.Get<CompressionMethod>("compression_method");
            } else {
                await WriteString("", cToken);
            }

            await WriteUInt((int)stage, cToken);
            await WriteUInt(_connectionSettings.Compress ? (int)compressionMethod : 0, cToken);
            await WriteString(sql, cToken);
            await _baseStream.FlushAsync(cToken);

            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables && noData) {
                await new Block().Write(this, cToken);
                await _baseStream.FlushAsync(cToken);
            }

            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables) await SendBlocks(xtables, cToken);
        } catch (Exception e) {
            _owner.MaybeSetBroken(e);
            throw;
        }
    }

    internal async Task<Block> ReadSchema(CancellationToken cToken) {
        try {
            var schema = new Response();
            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithColumnDefaultsMetadata) {
                await ReadPacket(schema, cToken);
                if (schema.Type == ServerMessageType.TableColumns)
                    await ReadPacket(schema, cToken);
            } else {
                await ReadPacket(schema, cToken);
            }

            return schema.Blocks.First();
        } catch (Exception e) {
            _owner.MaybeSetBroken(e);
            throw;
        }
    }

    internal async Task SendBlocks(IEnumerable<Block> blocks, CancellationToken cToken) {
        try {
            if (blocks != null)
                foreach (var block in blocks) {
                    await block.Write(this, cToken);
                    await _baseStream.FlushAsync(cToken);
                }

            await new Block().Write(this, cToken);
            await _baseStream.FlushAsync(cToken);
        } catch (Exception e) {
            _owner.MaybeSetBroken(e);
            throw;
        }
    }

    internal async Task<Response> ReadResponse(CancellationToken cToken) {
        try {
            var rv = new Response();
            while (true)
                if (!await ReadPacket(rv, cToken))
                    break;

            return rv;
        } catch (Exception e) {
            _owner.MaybeSetBroken(e);
            throw;
        }
    }

    internal async Task<Block> ReadBlock(CancellationToken cToken) {
        try {
            var rv = new Response();
            while (await ReadPacket(rv, cToken))
                if (rv.Blocks.Any())
                    return rv.Blocks.First();
            return null;
        } catch (Exception e) {
            _owner.MaybeSetBroken(e);
            throw;
        }
    }

    internal async Task<bool> ReadPacket(Response rv, CancellationToken cToken) {
        var type = (ServerMessageType)await ReadUInt(cToken);
        rv.Type = type;
        switch (type) {
            case ServerMessageType.Data:
            case ServerMessageType.Totals:
            case ServerMessageType.Extremes:
                rv.AddBlock(await Block.Read(this, cToken));
                return true;
            case ServerMessageType.Exception:
                await ReadAndThrowException(cToken);
                return false;
            case ServerMessageType.Progress: {
                var rows = await ReadUInt(cToken);
                var bytes = await ReadUInt(cToken);
                long total = 0;
                if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTotalRowsInProgress)
                    total = await ReadUInt(cToken);
                rv.OnProgress(rows, total, bytes);
                return true;
            }
            case ServerMessageType.ProfileInfo: {
                /*var rows = */
                await ReadUInt(cToken);
                /*var blocks = */
                await ReadUInt(cToken);
                /*var bytes = */
                await ReadUInt(cToken);
                /*var appliedLimit =*/
                await ReadUInt(cToken); //bool
                /*var rowsNoLimit = */
                await ReadUInt(cToken);
                /*var calcRowsNoLimit = */
                await ReadUInt(cToken); //bool
                return true;
            }
            case ServerMessageType.TableColumns: {
                /*var empty = */
                await ReadString(cToken);
                /*var columns = */
                await ReadString(cToken);
            }
                return true;
            case ServerMessageType.Pong:
                return true;
            case ServerMessageType.EndOfStream:
                rv.OnEnd();
                return false;
            default:
                throw new InvalidOperationException($"Received unknown packet type {type} from server.");
        }
    }

    public static string EscapeName(string str) {
        if (!NameRegex.IsMatch(str)) throw new ArgumentException($"'{str}' is invalid identifier.");
        return str;
    }

    public static string EscapeStringValue(string str) => "\'" + str.Replace("\\", "\\\\").Replace("\'", "\\\'") + "\'";

    public void Close() {
        if (_compStream != null)
            _compStream.Dispose();
    }

    public static string UnescapeStringValue(string src) {
        if (src == null) return string.Empty;
        if (src.StartsWith("'") && src.EndsWith("'")) return src.Substring(1, src.Length - 2).Replace("\\'", "'").Replace("\\\\", "\\");
        return src;
    }

    #region Structures IO

    private async Task ReadAndThrowException(CancellationToken cToken) => throw await ReadException(cToken);

    private async Task<Exception> ReadException(CancellationToken cToken) {
        var code = BitConverter.ToInt32(await ReadBytes(4, -1, cToken), 0); // reader.ReadInt32();
        var name = await ReadString(cToken);
        var message = await ReadString(cToken);
        var stackTrace = await ReadString(cToken);
        var nested = (await ReadBytes(1, -1, cToken)).Any(x => x != 0);
        if (nested)
            return new ClickHouseException(message, await ReadException(cToken)) {
                Code = code,
                Name = name,
                ServerStackTrace = stackTrace
            };
        return new ClickHouseException(message) {
            Code = code,
            Name = name,
            ServerStackTrace = stackTrace
        };
    }

    #endregion

    #region Low-level IO

    internal Task WriteByte(byte b, CancellationToken cToken) => _ioStream.WriteAsync(new[] { b }, 0, 1, cToken);

    internal async Task WriteUInt(long s, CancellationToken cToken) {
        var x = (ulong)s;
        for (var i = 0; i < 9; i++) {
            var b = (byte)((byte)x & 0x7f);
            if (x > 0x7f)
                b |= 0x80;
            await WriteByte(b, cToken);
            x >>= 7;
            if (x == 0) return;
        }
    }

    internal async Task<long> ReadUInt(CancellationToken cToken) {
        var x = 0;
        for (var i = 0; i < 9; ++i) {
            var b = await ReadByte(cToken);
            x |= (b & 0x7F) << (7 * i);

            if ((b & 0x80) == 0) return x;
        }

        return x;
    }

    internal async Task WriteString(string s, CancellationToken cToken) {
        if (s == null) s = "";
        var bytes = Encoding.UTF8.GetBytes(s);
        await WriteUInt((uint)bytes.Length, cToken);
        await WriteBytes(bytes, cToken);
    }

    internal async Task<string> ReadString(CancellationToken cToken) {
        var len = await ReadUInt(cToken);
        if (len > int.MaxValue)
            throw new ArgumentException("Server sent too long string.");
        var rv = len == 0 ? string.Empty : Encoding.UTF8.GetString(await ReadBytes((int)len, -1, cToken));
        return rv;
    }

    public async Task<byte[]> ReadBytes(int i, int size, CancellationToken cToken) {
        var bytes = new byte[size == -1 ? i : size];
        var read = 0;

        do {
            var cur = await _ioStream.ReadAsync(bytes, read, i - read, cToken);
            read += cur;

            if (cur == 0) throw new EndOfStreamException();
        } while (read < i);

        return bytes;
    }

    public async Task<byte> ReadByte(CancellationToken cToken) => (await ReadBytes(1, -1, cToken))[0];

    public Task WriteBytes(byte[] bytes, CancellationToken cToken) => _ioStream.WriteAsync(bytes, 0, bytes.Length, cToken);

    public Task WriteBytes(byte[] bytes, int offset, int len, CancellationToken cToken) => _ioStream.WriteAsync(bytes, offset, len, cToken);

    #endregion

    #region Compression

    internal class CompressionHelper : IDisposable {
        private readonly ProtocolFormatter _formatter;

        public CompressionHelper(ProtocolFormatter formatter) {
            _formatter = formatter;
            formatter.StartCompression();
        }

        public void Dispose() => _formatter.EndCompression();
    }

    internal class DecompressionHelper : IDisposable {
        private readonly ProtocolFormatter _formatter;

        public DecompressionHelper(ProtocolFormatter formatter) {
            _formatter = formatter;
            formatter.StartDecompression();
        }

        public void Dispose() => _formatter.EndDecompression();
    }

    private void StartCompression() {
        if (_connectionSettings.Compress) {
            Debug.Assert(_compStream == null, "Already doing compression/decompression!");
            _compStream = _compressor.BeginCompression(_baseStream);
            _ioStream = _compStream;
        }
    }

    private void EndCompression() {
        if (_connectionSettings.Compress) {
            Debug.Assert(_compStream != null, "Compression has not been started!");
            _compressor.EndCompression();
            _compStream = null;
            _ioStream = _baseStream;
        }
    }

    private void StartDecompression() {
        if (_connectionSettings.Compress) {
            Debug.Assert(_compStream == null, "Already doing compression/decompression!");
            _compStream = _compressor.BeginDecompression(_baseStream);
            _ioStream = _compStream;
        }
    }

    private void EndDecompression() {
        if (_connectionSettings.Compress) {
            Debug.Assert(_compStream != null, "Compression has not been started!");
            _compressor.EndDecompression();
            _compStream = null;
            _ioStream = _baseStream;
        }
    }

    internal CompressionHelper Compression => new(this);
    internal DecompressionHelper Decompression => new(this);

    #endregion
}
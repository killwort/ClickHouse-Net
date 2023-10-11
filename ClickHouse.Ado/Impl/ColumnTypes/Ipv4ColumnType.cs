using System;
using System.Collections;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes;

internal class Ipv4ColumnType : ColumnType {
    public const string Ipv4ColumnTypeName = "IPv4";

    public Ipv4ColumnType() { }
    public Ipv4ColumnType(IPAddress[] data) => Data = data;
    public IPAddress[] Data { get; private set; }
    public override int Rows => Data?.Length ?? 0;
    internal override Type CLRType => typeof(IPAddress);

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        var itemSize = 4;
        var xdata = new IPAddress[rows];
        for (var i = 0; i < rows; i++) {
            var bytes = await formatter.ReadBytes(itemSize, -1, cToken);
            xdata[i] = new IPAddress(bytes);
        }

        Data = xdata;
    }

    public override void ValueFromConst(Parser.ValueType val) {
        switch (val.TypeHint) {
            case Parser.ConstType.String:
                if (IPAddress.TryParse(val.StringValue.Trim('\''), out var parsed) && parsed.AddressFamily == AddressFamily.InterNetwork)
                    Data = new[] { parsed };
                else throw new InvalidCastException("Cannot convert value to ipv4 address.");
                break;
            case Parser.ConstType.Number:
                if (long.TryParse(val.StringValue, out var parsedInt))
                    Data = new[] { new IPAddress(parsedInt) };
                else throw new InvalidCastException("Cannot convert value to ipv4 address.");
                break;
            default:
                throw new InvalidCastException("Cannot convert value to ipv4 address.");
        }
    }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => Ipv4ColumnTypeName;

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        foreach (var d in Data) {
            var ipBytes = d.GetAddressBytes();
            Debug.Assert(ipBytes.Length == 4);
            await formatter.WriteBytes(ipBytes, cToken);
        }
    }

    public override void ValueFromParam(ClickHouseParameter parameter) {
        switch (parameter.DbType) {
            case DbType.Binary:
                if (parameter.Value is byte[] bytes)
                    Data = new[] { new IPAddress(bytes) };
                else if (parameter.Value is IPAddress addr)
                    Data = new[] { addr };
                else if (parameter.Value is int numI)
                    Data = new[] { new IPAddress(numI) };
                else if (parameter.Value is uint numUI)
                    Data = new[] { new IPAddress(numUI) };
                else if (parameter.Value is long numL)
                    Data = new[] { new IPAddress(numL) };
                else
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to ipv4.");
                break;
            default:
                throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to ipv4.");
        }
    }

    public override object Value(int currentRow) => Data[currentRow];

    public override long IntValue(int currentRow) => BitConverter.ToInt64(Data[currentRow].GetAddressBytes(), 0);

    public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<IPAddress>().ToArray();

    public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<IPAddress>().Select(x => x ?? IPAddress.None).ToArray();
}
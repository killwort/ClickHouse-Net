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

internal class Ipv6ColumnType : ColumnType {
    public const string Ipv6ColumnTypeName = "IPv6";

    public Ipv6ColumnType() { }
    public Ipv6ColumnType(IPAddress[] data) => Data = data;

    public IPAddress[] Data { get; private set; }
    public override int Rows => Data?.Length ?? 0;
    internal override Type CLRType => typeof(IPAddress);

    internal override async Task Read(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        var itemSize = 16;
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
                if (IPAddress.TryParse(val.StringValue.Trim('\''), out var parsed) && parsed.AddressFamily == AddressFamily.InterNetworkV6)
                    Data = new[] { parsed };
                else throw new InvalidCastException("Cannot convert value to ipv6 address.");
                break;
            default:
                throw new InvalidCastException("Cannot convert value to ipv6 address.");
        }
    }

    public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => Ipv6ColumnTypeName;

    public override async Task Write(ProtocolFormatter formatter, int rows, CancellationToken cToken) {
        foreach (var d in Data) {
            var ipBytes = d.GetAddressBytes();
            Debug.Assert(ipBytes.Length == 16);
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
                else
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to ipv6.");
                break;
            default:
                throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to ipv6.");
        }
    }

    public override object Value(int currentRow) => Data[currentRow];

    public override long IntValue(int currentRow) => BitConverter.ToInt64(Data[currentRow].GetAddressBytes(), 0);

    public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<IPAddress>().ToArray();

    public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<IPAddress?>().Select(x => x ?? IPAddress.None).ToArray();
}

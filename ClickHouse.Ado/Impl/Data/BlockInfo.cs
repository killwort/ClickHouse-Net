using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data; 

internal class BlockInfo {
    public bool IsOwerflow { get; private set; }
    public int BucketNum { get; private set; } = -1;

    internal async Task Write(ProtocolFormatter formatter, CancellationToken cToken) {
        await formatter.WriteUInt(1, cToken);
        await formatter.WriteByte(IsOwerflow ? (byte)1 : (byte)0, cToken);
        await formatter.WriteUInt(2, cToken);
        await formatter.WriteBytes(BitConverter.GetBytes(BucketNum), cToken);
        await formatter.WriteUInt(0, cToken);
    }

    public static async Task<BlockInfo> Read(ProtocolFormatter formatter, CancellationToken cToken) {
        long fieldNum;
        var rv = new BlockInfo();

        while ((fieldNum = await formatter.ReadUInt(cToken)) != 0)
            switch (fieldNum) {
                case 1:
                    rv.IsOwerflow = await formatter.ReadByte(cToken) != 0;
                    break;
                case 2:
                    rv.BucketNum = BitConverter.ToInt32(await formatter.ReadBytes(4, -1, cToken), 0);
                    break;
                default:
                    throw new InvalidOperationException("Unknown field number {0} in block info.");
            }

        return rv;
    }
}
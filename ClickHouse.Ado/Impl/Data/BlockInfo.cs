using System;

namespace ClickHouse.Ado.Impl.Data {
    internal class BlockInfo {
        public bool IsOwerflow { get; private set; }
        public int BucketNum { get; private set; } = -1;

        internal void Write(ProtocolFormatter formatter) {
            formatter.WriteUInt(1);
            formatter.WriteByte(IsOwerflow ? (byte) 1 : (byte) 0);
            formatter.WriteUInt(2);
            formatter.WriteBytes(BitConverter.GetBytes(BucketNum));
            formatter.WriteUInt(0);
        }

        public static BlockInfo Read(ProtocolFormatter formatter) {
            long fieldNum;
            var rv = new BlockInfo();

            while ((fieldNum = formatter.ReadUInt()) != 0)
                switch (fieldNum) {
                    case 1:
                        rv.IsOwerflow = formatter.ReadBytes(1)[0] != 0;
                        break;
                    case 2:
                        rv.BucketNum = BitConverter.ToInt32(formatter.ReadBytes(4), 0);
                        break;
                    default:
                        throw new InvalidOperationException("Unknown field number {0} in block info.");
                }

            return rv;
        }
    }
}
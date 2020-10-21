using System.Collections.Generic;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl {
    internal class Response {
        public ServerMessageType Type;
        public List<Block> Blocks { get; } = new List<Block>();

        public void AddBlock(Block block) => Blocks.Add(block);

        public void OnProgress(long rows, long total, long bytes) { }

        public void OnEnd() { }
    }
}
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test {
    [TestFixture]
    public class Test_90_CloseConnection {
        [Test]
        public void TestConnectionAutoClose() {
            using (var cnn = ConnectionHandler.GetConnection()) {
                using (var cmd = cnn.CreateCommand("SELECT * FROM system.databases"))
                using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection)) {
                    reader.ReadAll(r => {  });
                }
            }
        }
    }
}

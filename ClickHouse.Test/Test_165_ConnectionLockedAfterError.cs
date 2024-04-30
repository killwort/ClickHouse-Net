using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado;
using NUnit.Framework;

namespace ClickHouse.Test;

public class Test_165_ConnectionLockedAfterError
{
    [Test]
    public async Task Test()
    {
        using (var cnn = ConnectionHandler.GetConnection())
        {
            try
            {
                await MakeError(cnn);
                Assert.Fail("Errorneous command didn't generate an error");
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOf<ClickHouseException>(ex);
            }
            try
            {
                await MakeError(cnn);
                Assert.Fail("Errorneous command didn't generate an error");
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOf<ClickHouseException>(ex);
            }
        }
    }

    private async Task MakeError(ClickHouseConnection cnn)
    {
        var cancellation = new CancellationTokenSource(5000);
        using (var cmd = cnn.CreateCommand(" SELECT from system.query_log  limit 10"))
        {
            DbDataReader reader = null;
            using (reader = await cmd.ExecuteReaderAsync(cancellation.Token))
                while (await reader.NextResultAsync(cancellation.Token))
                while (await reader.ReadAsync(cancellation.Token))
                    Console.WriteLine(reader.GetValue(0));
        }
    }
}

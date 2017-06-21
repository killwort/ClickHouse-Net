#if NETCOREAPP11
namespace ClickHouse.Ado
{
    public enum DbType
    {
        Unknown,
        String,
        DateTime,
        Date,
        Integral,
        Float
    }
}
#endif
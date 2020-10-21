namespace ClickHouse.Ado.Impl {
    internal static class ProtocolCaps {
        internal const int DbmsMinRevisionWithTemporaryTables = 50264;
        internal const int DbmsMinRevisionWithTotalRowsInProgress = 51554;
        internal const int DbmsMinRevisionWithBlockInfo = 51903;
        internal const int DbmsMinRevisionWithClientInfo = 54032;
        internal const int DbmsMinRevisionWithServerTimezone = 54058;
        internal const int DbmsMinRevisionWithQuotaKeyInClientInfo = 54060;
        internal const int DbmsMinRevisionWithServerDisplayName = 54372;
        internal const int DbmsMinRevisionWithServerVersionPatch = 54401;
        internal const int DbmsMinRevisionWithColumnDefaultsMetadata = 54410;
        internal const int DbmsTcpProtocolVersion = 54060;
        internal const string ClientName = "ClickHouse .NET client library";
    }
}
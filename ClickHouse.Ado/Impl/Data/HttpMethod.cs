namespace ClickHouse.Ado.Impl.Data;

/// <summary>
///     HTTP method in the <see cref="ClientInfo" />.
/// </summary>
public enum HttpMethod {
    /// <summary>
    ///     Unknown
    /// </summary>
    Unknown = 0,

    /// <summary>
    ///     GET
    /// </summary>
    Get = 1,

    /// <summary>
    ///     POST
    /// </summary>
    Post = 2
}
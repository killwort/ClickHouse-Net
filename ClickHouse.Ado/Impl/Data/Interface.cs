namespace ClickHouse.Ado.Impl.Data;

/// <summary>
///     Protocol type in the <see cref="ClientInfo" />.
/// </summary>
public enum Interface {
    /// <summary>
    ///     Native
    /// </summary>
    Tcp = 1,

    /// <summary>
    ///     HTTP
    /// </summary>
    Http = 2
}

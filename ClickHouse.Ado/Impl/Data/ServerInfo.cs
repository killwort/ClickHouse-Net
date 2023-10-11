namespace ClickHouse.Ado.Impl.Data;

/// <summary>
///     Information about connected server.
/// </summary>
public class ServerInfo {
    /// <summary>
    ///     Server build number.
    /// </summary>
    public long Build;

    /// <summary>
    ///     Server display name.
    /// </summary>
    public string DisplayName;

    /// <summary>
    ///     Server major version number.
    /// </summary>
    public long Major;

    /// <summary>
    ///     Server minor version number.
    /// </summary>
    public long Minor;

    /// <summary>
    ///     Server name.
    /// </summary>
    public string Name;

    /// <summary>
    ///     Server patch number.
    /// </summary>
    public long Patch;

    /// <summary>
    ///     Server time zone.
    /// </summary>
    public string Timezone;
}

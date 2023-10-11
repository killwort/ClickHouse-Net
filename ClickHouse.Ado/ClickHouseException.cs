using System;

namespace ClickHouse.Ado;

/// <summary>
///     Clickhouse specific exception.
/// </summary>
public class ClickHouseException : Exception {
    /// <summary>
    ///     Exception code.
    /// </summary>
    public int Code;

    /// <summary>
    ///     Exception name.
    /// </summary>
    public string Name;

    /// <summary>
    ///     Server stack trace.
    /// </summary>
    public string ServerStackTrace;

    /// <summary>
    ///     Creates empty exception.
    /// </summary>
    public ClickHouseException() { }

    /// <summary>
    ///     Creates exception with message.
    /// </summary>
    /// <param name="message">Message.</param>
    public ClickHouseException(string message) : base(message) { }

    /// <summary>
    ///     Creates exception with message and inner exception.
    /// </summary>
    /// <param name="message">Message.</param>
    /// <param name="innerException">Inner exception.</param>
    public ClickHouseException(string message, Exception innerException) : base(message, innerException) { }
}
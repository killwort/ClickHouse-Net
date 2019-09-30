using System.Collections.Generic;

namespace ClickHouse.Ado {
    /// <summary>
    /// Enumeration helped for bulk inserts.
    /// </summary>
    /// <remarks>Using <see cref="IBulkInsertEnumerable"/> allows you to avoid mass collection copying on inserts.</remarks>
    public interface IBulkInsertEnumerable {
        /// <summary>
        /// Get enumerable data by column.
        /// </summary>
        /// <param name="colNumber">Number of column.</param>
        /// <param name="columnName">Name of column.</param>
        /// <param name="schemaType">Column type as fetched from clickhouse command schema.</param>
        /// <returns>List of values for specified column.</returns>
        IEnumerable<object> GetColumnData(int colNumber, string columnName, string schemaType);
    }
}
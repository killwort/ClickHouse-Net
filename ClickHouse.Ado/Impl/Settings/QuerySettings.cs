using System;
using System.Collections.Generic;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.Settings {
    internal class QuerySettings {
        private static readonly Dictionary<string, SettingValue> SettingDefaults = new Dictionary<string, SettingValue> {
            /** При записи данных, для сжатия выделяется буфер размером max_compress_block_size. При переполнении буфера или если в буфер */
            /** записано данных больше или равно, чем min_compress_block_size, то при очередной засечке, данные так же будут сжиматься */
            /** В результате, для маленьких столбцов (числа 1-8 байт), при index_granularity = 8192, размер блока будет 64 KБ. */
            /** А для больших столбцов (Title - строка ~100 байт), размер блока будет ~819 КБ.  */
            /** За счёт этого, коэффициент сжатия почти не ухудшится.  */
            {"min_compress_block_size", new UInt64SettingValue(65536)},
            {"max_compress_block_size", new UInt64SettingValue(1048576)},
            /** Максимальный размер блока для чтения */
            {"max_block_size", new UInt64SettingValue(65536)},
            /** Максимальный размер блока для вставки, если мы управляем формированием блоков для вставки. */
            {"max_insert_block_size", new UInt64SettingValue(1048576)},
            /** Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough. */
            {"min_insert_block_size_rows", new UInt64SettingValue(1048576)},
            /** Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough. */
            {"min_insert_block_size_bytes", new UInt64SettingValue(1048576 * 256)},
            /** Максимальное количество потоков выполнения запроса. По-умолчанию - определять автоматически. */
            {"max_threads", new UInt64SettingValue(0)},
            /** Максимальный размер буфера для чтения из файловой системы. */
            {"max_read_buffer_size", new UInt64SettingValue(1048576)},
            /** Максимальное количество соединений при распределённой обработке одного запроса (должно быть больше, чем max_threads). */
            {"max_distributed_connections", new UInt64SettingValue(1024)},
            /** Какую часть запроса можно прочитать в оперативку для парсинга (оставшиеся данные для INSERT, если есть, считываются позже) */
            {"max_query_size", new UInt64SettingValue(262144)},
            /** Интервал в микросекундах для проверки, не запрошена ли остановка выполнения запроса, и отправки прогресса. */
            {"interactive_delay", new UInt64SettingValue(100000)},
            {"connect_timeout", new TimeSpanSettingValue(10)},
            /** Если следует выбрать одну из рабочих реплик. */
            {"connect_timeout_with_failover_ms", new TimeSpanMsSettingValue(50)},
            {"receive_timeout", new TimeSpanSettingValue(400)},
            {"send_timeout", new TimeSpanSettingValue(400)},
            /** Время ожидания в очереди запросов, если количество одновременно выполняющихся запросов превышает максимальное. */
            {"queue_max_wait_ms", new TimeSpanMsSettingValue(5000)},
            /** Блокироваться в цикле ожидания запроса в сервере на указанное количество секунд. */
            {"poll_interval", new UInt64SettingValue(10)},
            /** Максимальное количество соединений с одним удалённым сервером в пуле. */
            {"distributed_connections_pool_size", new UInt64SettingValue(1024)},
            /** Максимальное количество попыток соединения с репликами. */
            {"connections_with_failover_max_tries", new UInt64SettingValue(3)},
            /** Считать минимумы и максимумы столбцов результата. Они могут выводиться в JSON-форматах. */
            {"extremes", new BoolSettingValue(false)},
            /** Использовать ли кэш разжатых блоков. */
            {"use_uncompressed_cache", new BoolSettingValue(true)},
            /** Следует ли отменять выполняющийся запрос с таким же id, как новый. */
            {"replace_running_query", new BoolSettingValue(false)},
            /** Количество потоков, выполняющих фоновую работу для таблиц (например, слияние в merge tree). 
              * TODO: Сейчас применяется только при запуске сервера. Можно сделать изменяемым динамически. */
            {"background_pool_size", new UInt64SettingValue(16)},

            /** Sleep time for StorageDistributed DirectoryMonitors in case there is no work or exception has been thrown */
            {"distributed_directory_monitor_sleep_time_ms", new TimeSpanMsSettingValue(100)},

            /** Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree */
            {"optimize_move_to_prewhere", new BoolSettingValue(true)},

            /** Ожидать выполнения действий по манипуляции с партициями. 0 - не ждать, 1 - ждать выполнения только у себя, 2 - ждать всех. */
            {"replication_alter_partitions_sync", new UInt64SettingValue(1)},
            /** Ожидать выполнения действий по изменению структуры таблицы в течение указанного количества секунд. 0 - ждать неограниченное время. */
            {"replication_alter_columns_timeout", new UInt64SettingValue(60)},
            {"load_balancing", new EnumSettingValue<LoadBalancing>(LoadBalancing.Random)},
            {"totals_mode", new EnumSettingValue<TotalsMode>(TotalsMode.AfterHavingExclusive)},
            {"totals_auto_threshold", new FloatSettingValue(0.5f)},

            /** Включена ли компиляция запросов. */
            {"compile", new BoolSettingValue(false)},
            /** Количество одинаковых по структуре запросов перед тем, как инициируется их компиляция. */
            {"min_count_to_compile", new UInt64SettingValue(3)},
            /** При каком количестве ключей, начинает использоваться двухуровневая агрегация. 0 - порог не выставлен. */
            {"group_by_two_level_threshold", new UInt64SettingValue(100000)},
            /** При каком размере состояния агрегации в байтах, начинает использоваться двухуровневая агрегация. 0 - порог не выставлен. 
              * Двухуровневая агрегация начинает использоваться при срабатывании хотя бы одного из порогов. */
            {"group_by_two_level_threshold_bytes", new UInt64SettingValue(100000000)},
            /** Включён ли экономный по памяти режим распределённой агрегации. */
            {"distributed_aggregation_memory_efficient", new BoolSettingValue(false)},
            /** Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 
              * 0 means - same as 'max_threads'. */
            {"aggregation_memory_efficient_merge_threads", new UInt64SettingValue(0)},

            /** Максимальное количество используемых реплик каждого шарда при выполнении запроса */
            {"max_parallel_replicas", new UInt64SettingValue(1)},
            {"parallel_replicas_count", new UInt64SettingValue(0)},
            {"parallel_replica_offset", new UInt64SettingValue(0)},

            /** Тихо пропускать недоступные шарды. */
            {"skip_unavailable_shards", new BoolSettingValue(false)},

            /** Не мерджить состояния агрегации с разных серверов при распределённой обработке запроса 
              *  - на случай, когда доподлинно известно, что на разных шардах разные ключи. 
              */
            {"distributed_group_by_no_merge", new BoolSettingValue(false)},

            /** Тонкие настройки для чтения из MergeTree */

            /** Если из одного файла читается хотя бы столько строк, чтение можно распараллелить. */
            {"merge_tree_min_rows_for_concurrent_read", new UInt64SettingValue(20 * 8192)},
            /** Можно пропускать чтение более чем стольки строк ценой одного seek по файлу. */
            {"merge_tree_min_rows_for_seek", new UInt64SettingValue(0)},
            /** Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их. */
            {"merge_tree_coarse_index_granularity", new UInt64SettingValue(8)},
            /** Максимальное количество строк на запрос, для использования кэша разжатых данных. Если запрос большой - кэш не используется. 
              * (Чтобы большие запросы не вымывали кэш.) */
            {"merge_tree_max_rows_to_use_cache", new UInt64SettingValue(1024 * 1024)},

            /** Распределять чтение из MergeTree по потокам равномерно, обеспечивая стабильное среднее время исполнения каждого потока в пределах одного чтения. */
            {"merge_tree_uniform_read_distribution", new BoolSettingValue(true)},

            /** Минимальная длина выражения expr = x1 OR ... expr = xN для оптимизации */
            {"optimize_min_equality_disjunction_chain_length", new UInt64SettingValue(3)},

            /** Минимальное количество байт для операций ввода/ввывода минуя кэш страниц. 0 - отключено. */
            {"min_bytes_to_use_direct_io", new UInt64SettingValue(0)},

            /** Кидать исключение, если есть индекс, и он не используется. */
            {"force_index_by_date", new BoolSettingValue(false)},
            {"force_primary_key", new BoolSettingValue(false)},

            /** В запросе INSERT с указанием столбцов, заполнять значения по-умолчанию только для столбцов с явными DEFAULT-ами. */
            {"strict_insert_defaults", new BoolSettingValue(false)},

            /** В случае превышения максимального размера mark_cache, удалять только записи, старше чем mark_cache_min_lifetime секунд. */
            {"mark_cache_min_lifetime", new UInt64SettingValue(10000)},

            /** Позволяет использовать больше источников, чем количество потоков - для более равномерного распределения работы по потокам. 
              * Предполагается, что это временное решение, так как можно будет в будущем сделать количество источников равное количеству потоков, 
              *  но чтобы каждый источник динамически выбирал себе доступную работу. 
              */
            {"max_streams_to_max_threads_ratio", new FloatSettingValue(1)},

            /** Позволяет выбирать метод сжатия данных при записи */
            {"network_compression_method", new EnumSettingValue<CompressionMethod>(CompressionMethod.Lz4)},

            /** Приоритет запроса. 1 - самый высокий, больше - ниже 0 - не использовать приоритеты. */
            {"priority", new UInt64SettingValue(0)},

            /** Логгировать запросы и писать лог в системную таблицу. */
            {"log_queries", new BoolSettingValue(false)},

            /** If query length is greater than specified threshold (in bytes)},, then cut query when writing to query log. 
              * Also limit length of printed query in ordinary text log. 
              */
            {"log_queries_cut_to_length", new UInt64SettingValue(100000)},

            /** Как выполняются распределённые подзапросы внутри секций IN или JOIN? */
            {"distributed_product_mode", new EnumSettingValue<DistributedProductMode>(DistributedProductMode.Deny)},

            /** Схема выполнения GLOBAL-подзапросов. */
            {"global_subqueries_method", new EnumSettingValue<GlobalSubqueriesMethod>(GlobalSubqueriesMethod.Push)},

            /** Максимальное количество одновременно выполняющихся запросов на одного user-а. */
            {"max_concurrent_queries_for_user", new UInt64SettingValue(0)},

            /** Для запросов INSERT в реплицируемую таблицу, ждать записи на указанное число реплик и лианеризовать добавление данных. 0 - отключено. */
            {"insert_quorum", new UInt64SettingValue(0)},
            {"insert_quorum_timeout", new TimeSpanMsSettingValue(600000)},
            /** Для запросов SELECT из реплицируемой таблицы, кидать исключение, если на реплике нет куска, записанного с кворумом 
              * не читать куски, которые ещё не были записаны с кворумом. */
            {"select_sequential_consistency", new UInt64SettingValue(0)},
            /** Максимальное количество различных шардов и максимальное количество реплик одного шарда в функции remote. */
            {"table_function_remote_max_addresses", new UInt64SettingValue(1000)},
            /** Маскимальное количество потоков при распределённой обработке одного запроса */
            {"max_distributed_processing_threads", new UInt64SettingValue(8)},

            /** Настройки понижения числа потоков в случае медленных чтений. */
            /** Обращать внимания только на чтения, занявшие не меньше такого количества времени. */
            {"read_backoff_min_latency_ms", new TimeSpanMsSettingValue(1000)},
            /** Считать события, когда пропускная способность меньше стольки байт в секунду. */
            {"read_backoff_max_throughput", new UInt64SettingValue(1048576)},
            /** Не обращать внимания на событие, если от предыдущего прошло меньше стольки-то времени. */
            {"read_backoff_min_interval_between_events_ms", new TimeSpanMsSettingValue(1000)},
            /** Количество событий, после которого количество потоков будет уменьшено. */
            {"read_backoff_min_events", new UInt64SettingValue(2)},

            /** В целях тестирования exception safety - кидать исключение при каждом выделении памяти с указанной вероятностью. */
            {"memory_tracker_fault_probability", new FloatSettingValue(0f)},

            /** Сжимать результат, если клиент по HTTP сказал, что он понимает данные, сжатые методом gzip или deflate */
            {"enable_http_compression", new BoolSettingValue(false)},
            /** Уровень сжатия - используется, если клиент по HTTP сказал, что он понимает данные, сжатые методом gzip или deflate */
            {"http_zlib_compression_level", new UInt64SettingValue(3)},

            /** При разжатии данных POST от клиента, сжатых родным форматом, не проверять чексуммы */
            {"http_native_compression_disable_checksumming_on_decompress", new BoolSettingValue(false)},

            /** Таймаут в секундах */
            {"resharding_barrier_timeout", new UInt64SettingValue(300)},

            /** What aggregate function to use for implementation of count(DISTINCT ...)}, */
            {"count_distinct_implementation", new StringSettingValue("uniq")},

            /** Write statistics about read rows, bytes, time elapsed in suitable output formats */
            {"output_format_write_statistics", new BoolSettingValue(true)},

            /** Write add http CORS header */
            {"add_http_cors_header", new BoolSettingValue(false)},

            /** Skip columns with unknown names from input data (it works for JSONEachRow and TSKV formats)},. */
            {"input_format_skip_unknown_fields", new BoolSettingValue(false)},

            /** For Values format: if field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression. */
            {"input_format_values_interpret_expressions", new BoolSettingValue(true)},

            /** Controls quoting of 64-bit integers in JSON output format. */
            {"output_format_json_quote_64bit_integers", new BoolSettingValue(true)},

            /** Rows limit for Pretty formats. */
            {"output_format_pretty_max_rows", new UInt64SettingValue(10000)},

            /** Use client timezone for interpreting DateTime string values, instead of adopting server timezone. */
            {"use_client_time_zone", new BoolSettingValue(false)},

            /** Send progress notifications using X-ClickHouse-Progress headers. 
              * Some clients do not support high amount of HTTP headers (Python requests in particular)},, so it is disabled by default. 
              */
            {"send_progress_in_http_headers", new BoolSettingValue(false)},

            /** Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval. */
            {"http_headers_progress_interval_ms", new UInt64SettingValue(100)},

            /** Do fsync after changing metadata for tables and databases (.sql files)},. 
              * Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem. 
              */
            {"fsync_metadata", new BoolSettingValue(true)},

            /** Maximum amount of errors while reading text formats (like CSV, TSV). 
              * In case of error, if both values are non-zero, 
              *  and at least absolute or relative amount of errors is lower than corresponding value, 
              *  will skip until next line and continue. 
              */
            {"input_format_allow_errors_num", new UInt64SettingValue(0)},
            {"input_format_allow_errors_ratio", new FloatSettingValue(0)}
        };

        private readonly Dictionary<string, SettingValue> _settings = new Dictionary<string, SettingValue>();

        public void Set(string name, ulong value) {
            var def = SettingDefaults[name];
            if (def.GetType() != typeof(UInt64SettingValue)) throw new InvalidCastException();
            _settings[name] = new UInt64SettingValue(value);
        }

        public void Set(string name, bool value) {
            var def = SettingDefaults[name];
            if (def.GetType() != typeof(BoolSettingValue)) throw new InvalidCastException();
            _settings[name] = new BoolSettingValue(value);
        }

        public void Set(string name, TimeSpan value) {
            var def = SettingDefaults[name];
            if (def.GetType() != typeof(TimeSpanSettingValue)) {
                if (def.GetType() != typeof(TimeSpanMsSettingValue)) throw new InvalidCastException();
                _settings[name] = new TimeSpanMsSettingValue(value);
                return;
            }

            _settings[name] = new TimeSpanSettingValue(value);
        }

        public void Set(string name, string value) {
            var def = SettingDefaults[name];
            if (def.GetType() != typeof(StringSettingValue)) throw new InvalidCastException();
            _settings[name] = new StringSettingValue(value);
        }

        public T Get<T>(string name, bool useDefaults, out bool isDefault) {
            isDefault = false;
            if (_settings.ContainsKey(name))
                return _settings[name].As<T>();
            isDefault = true;
            if (useDefaults)
                return SettingDefaults[name].As<T>();
            return default;
        }

        public T Get<T>(string name, bool useDefaults = true) => Get<T>(name, useDefaults, out _);

        public object Get(string name, bool useDefaults, out bool isDefault) {
            isDefault = false;
            if (_settings.ContainsKey(name))
                return _settings[name].AsValue();
            isDefault = true;
            if (useDefaults)
                return SettingDefaults[name].AsValue();
            return null;
        }

        public object Get(string name, bool useDefaults = true) => Get(name, useDefaults, out _);

        internal void Write(ProtocolFormatter formatter) {
            foreach (var settingValue in _settings) {
                formatter.WriteString(settingValue.Key);
                settingValue.Value.Write(formatter);
            }

            formatter.WriteString("");
        }
    }
}
# ClickHouse.ADO
.NET driver for [Yandex ClickHouse](http://clickhouse.yandex). This driver implements native ClickHouse protocol, shamelessly ripped out of original ClickHouse sources. In some ways it does not comply to ADO.NET 
rules however this is intentional.


А ещё есть описание по-русски, см. ниже.
## Changelog
v.2.0.3: Added support for `INSERT ... SETTINGS setting=value` syntax.

v.2.0.2.1 and v.2.0.2.2: Added net461 target and downgraded K4os.Compression.LZ4 requirement for it.

v.2.0.2: Switched to async IO, implemented System.Data.Common stuff like DbProviderFactory. Added support for IPv4 and IPv6 columns.

1.5.6-no-polling-on-tls: Backported changes from 2.0.3.

v.1.5.5-no-polling-on-tls: Patched a bug preventing SSL/TLS secured connections from working properly.

v.1.5.5: Added support for Bool type.

v.1.5.3: Fixed errors reading empty arrays.

v.1.5.2: Added support for Date32 type.

v.1.5.1: Introduced new way to handle Clickhouse's `Tuple` type. Now values are read as `System.Tuple<>` instead of `System.Object[]`. That change made possible reading of `Array(Tuple(...))` typed values.

v.1.4.0: Fixed query parsing when values are escaped.

v.1.3.1: Added support for LowCardinality type. Extended support for Decimal types.

v.1.2.6: Added (quite limited) support for timezones.
## Important usage notes
### SSL/TLS support
In order to wrap your clickhouse connection in SSL/TLS tunnel you should enable is on your server first (`tcp_port_secure` setting in the config.xml) and add `Encrypt=True` to the connection string (do not forget to change port number).

SSL/TLS was not working properly before 1.5.5 and led to infinite wait for data to arrive. It was 'patched' in 1.5.5-no-polling-on-tls version and completely mitigated in 2.x+.
### Raw SQL debug output
If you'd like to see all queries emitted by the driver to the server add `Trace=True` to the connection string and set up a .NET trace listener for the category `ClickHouse.Ado`.  
### No multiple queries
ClickHouse engine does not support parsing multiple queries per on `IDbCommand.Execute*` roundtrip. Please split your queries into separately executed commands.

### Always use NextResult
Although you may think that `NextResult` would not be used due to aforementioned lack of multiple query support that's completely wrong! You **must always use** `NextResult` 
as ClickHouse protocol and engine *may and will* return multiple resultsets per query and sometime result schemas may differ (definetly in regard to field 
ordering if query doesn't explicitly specify it).

### Hidden bulk-insert functionality
If you read ClickHouse documentation it strongly advices you to insert records in bulk (1000+ per request). This driver can do bulk inserts. To do so you have to use special
insert syntax:

```SQL
INSERT INTO some_table (col1, col2, col3) VALUES @bulk
```

And after that you must add parameted named `bulk` with its `Value` castable to `IEnumerable` each item of it must be `IEnumerable` too. Empty lists are not allowed.
Alternatively you may pass `IBulkInsertEnumerable` implementation as a `bulk`'s value to speed up processing and use less memory inside clickhouse driver.
This may be used conviniently with the following syntax:

```SQL
CREATE TABLE test (date Date, time DateTime, str String, int UInt16) ENGINE=MergeTree(date,(time,str,int), 8192)
```

```C#
class MyPersistableObject:IEnumerable{
	public string MyStringField;
	public DateTime MyDateField;
	public int MyIntField;

	//Count and order of returns must match column order in SQL INSERT
	public IEnumerator GetEnumerator(){
		yield return MyDateField;
		yield return MyDateField;
		yield return MyStringField;
		yield return (ushort)MyIntField;
	}
}

//... somewhere elsewhere ...
var list=new List<MyPersistableObject>();

// fill the list to insert
list.Add(new MyPersistableObject());

var command=connection.CreateCommand();
command.CommandText="INSERT INTO test (date,time,str,int) VALUES @bulk";
command.Parameters.Add(new ClickHouseParameter{
	ParameterName="bulk",
	Value=list
});
command.ExecuteNonQuery();
```

## Extending and deriving
If you've fixed some bugs or wrote some useful addition to this driver, please, do pull request them back here. 

If you need some functionality or found a bug but unable to implement/fix it, please file a ticket here, on GitHub.

# ClickHouse.ADO по-русски
.NET драйвер для [Yandex ClickHouse](http://clickhouse.yandex). В отличие от официального JDBC клиента этот драйвер не является обёрткой поверх ClickHouse HTTP, а реализует нативный протокол. Протокол (и части его реализации) нагло выдраны из исходников самого ClickHouse. В некоторых случаях этот драйвер ведёт себя не так, как обычные ADO.NET драйверы, это сделано намеренно и связано со спецификой ClickHouse.

## Важные изменения
v.2.0.2.1 и v.2.0.2.2: Добавлена цель net461 и для неё снижено требование к версии K4os.Compression.LZ4.

v.2.0.2: Переход на асинхронный ввод/вывод, реализация всякого из System.Data.Common, в том числе DbProviderFactory. Добавлена поддержка типов IPv4 and IPv6.

v.1.5.5-no-polling-on-tls: "Исправлен" баг с повисаниями на соединениях использующих SSL/TLS.

v.1.5.5: Добавлена поддержка типа Bool.

v.1.5.3: Исправлена ошибка чтения пустых массивов.

v.1.5.2: Добавлена поддержка типа Date32.

v.1.5.1. Изменён формат чтения/записи Clickhouse типа `Tuple`. Теперь значения читаются как `System.Tuple<>` вместо используемого ранее `System.Object[]`. Это изменение позволяет читать колонки типа `Array(Tuple(...))`, что раньше было не возможно из-за ошибок.

v.1.4.0: Исправлен разбор запросов с значениями содержащими экранированные символы.

v.1.3.1: Добавлена поддержка типа LowCardinality. Улучшена работа с типами Decimal.

v.1.2.6: Добавлена ограниченная поддержка временных зон.
## Прочти это перед использованием
### Поддержка SSL/TLS
Чтобы завернуть протокол кликхауса в SSL/TLS тунель надо, во-первых, включить SSL на сервере (настройка `tcp_port_secure` в config.xml), и, затем, добавить `Encrypt=True` в строку соединения (также не забыть сменить используемый номер порта).

До версии 1.5.5-no-polling-on-tls SSL/TLS нормально не работал и приводил к зависаниям. В 1.5.5-no-polling-on-tls это было "исправлено", а в версиях 2.х+ полностью устранено.
### Отладочный вывод SQL
Если хочется видеть какие SQL драйвер посылает серверу, то в строку соединения надо добавить `Trace=True` и включить слушатель трассировки для категории `ClickHouse.Ado`.
### Нет поддержки нескольких запросов
Движок ClickHouse не умеет обрабатывать несколько SQL запросов за один вызов `IDbCommand.Execute*`. Запросы надо разбивать на отдельные команды.

### Всегда используй NextResult
В связи с вышесказаным может показаться что `NextResult` не нужен, но это совершенно не так. Использование `NextResult` **обязательно**, поскольку протокол и движок ClickHouse *может и будет* возвращать несколько наборов данных на один запрос, и, хуже того, схемы этих наборов могут различаться (по крайней мере может быть перепутан порядок полей, если запрос не имеет явного указания порядка).

### Секретная функция групповой вставки
В документации ClickHouse указано, что вставлять данные лучше пачками 100+ записей. Для этого предусмотрен специальный синтаксис:

```SQL
INSERT INTO some_table (col1, col2, col3) VALUES @bulk
```

Для этой команды надо задать параметр `bulk` со значением `Value` приводимым к `IEnumerable`, каждый из элементов которого, в свою очередь, тоже должен быть `IEnumerable`.
Кроме того, в качестве значения параметра `bulk` передать объект реализующий `IBulkInsertEnumerable` - это уменьшит использование памяти и процессора внутри драйвера clickhouse.
Это удобно при использовании такого синтаксиса:

```SQL
CREATE TABLE test (date Date, time DateTime, str String, int UInt16) ENGINE=MergeTree(date,(time,str,int), 8192)
```

```C#
class MyPersistableObject:IEnumerable{
	public string MyStringField;
	public DateTime MyDateField;
	public int MyIntField;

	//Количество и порядок return должны соответствовать количеству и порядку полей в SQL INSERT
	public IEnumerator GetEnumerator(){
		yield return MyDateField;
		yield return MyDateField;
		yield return MyStringField;
		yield return (ushort)MyIntField;
	}
}

//... где-то ещё ...
var list=new List<MyPersistableObject>();

// заполнение списка вставляемых объектов
list.Add(new MyPersistableObject());

var command=connection.CreateCommand();
command.CommandText="INSERT INTO test (date,time,str,int) VALUES @bulk";
command.Parameters.Add(new ClickHouseParameter{
	ParameterName="bulk",
	Value=list
});
command.ExecuteNonQuery();
```

## Расширение и наследование
Если вы исправили баг или реализовали какую-то фичу, пожалуйста, сделайте pull request в этот репозиторий.

Если вам не хватает какой-то функции или вы нашли баг, который не можете исправить, напишите тикет здесь, на GitHub.

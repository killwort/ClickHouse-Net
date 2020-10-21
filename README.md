# ClickHouse.ADO
.NET driver for [Yandex ClickHouse](http://clickhouse.yandex). This driver implements native ClickHouse protocol, shamelessly ripped out of original ClickHouse sources. In some ways it does not comply to ADO.NET 
rules however this is intentional.


А ещё есть описание по-русски, см. ниже.

## Important usage notes
### No multiple queries
ClickHouse engine does not support parsing multiple queries per on `IDbCommand.Execute*` roundtrip. Please split your queries into separately executed commands.

### Always use NextResult
Although you may think that `NextResult` would not be used due to aforementioned lack of multiple query support that's completely wrong! You **must always use** `NextResult` 
as ClickHouse protocol and engine *may and will* return multiple resultsets per query and sometime result schemas may differ (definetly in regard to field 
ordering if query doesn't explicitly specify it).

### Hidden bulk-insert functionality
If you read ClickHouse documentation it stongly advices you to insert records in bulk (1000+ per request). This driver can do bulk inserts. To do so you have to use special
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

## Прочти это перед использованием
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

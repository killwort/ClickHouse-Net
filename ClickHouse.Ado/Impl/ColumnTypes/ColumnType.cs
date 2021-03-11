using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
using Scanner = ClickHouse.Ado.Impl.ATG.IdentList.Scanner;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal abstract class ColumnType {
        private static readonly Dictionary<string, string> CaseInsensitiveTypeAliases = new Dictionary<string, string>(new CaseInsensitiveComparer()) {
            //SQL-compatibility aliases.
            {"LONGBLOB", "String"},
            {"MEDIUMBLOB", "String"},
            {"TINYBLOB", "String"},
            {"BIGINT", "Int64"},
            {"SMALLINT", "Int16"},
            {"TIMESTAMP", "DateTime"},
            {"INTEGER", "Int32"},
            {"INT", "Int32"},
            {"DOUBLE", "Float64"},
            {"MEDIUMTEXT", "String"},
            {"TINYINT", "Int8"},
            {"DEC", "Decimal"},
            {"BINARY", "FixedString"},
            {"FLOAT", "Float32"},
            {"CHAR", "String"},
            {"VARCHAR", "String"},
            {"TEXT", "String"},
            {"TINYTEXT", "String"},
            {"LONGTEXT", "String"},
            {"BLOB", "String"},

            //Clickhouse-specific aliases
            {"Decimal", "Decimal"},
            {"Decimal64", "Decimal64"},
            {"Decimal32", "Decimal32"},
            {"Decimal128", "Decimal128"},
            {"Decimal256", "Decimal256"},
            {"Date", "Date"},
            {"DateTime", "DateTime"}
        };

        private static readonly Dictionary<string, Type> Types = new Dictionary<string, Type> {
            {"UInt8", typeof(SimpleColumnType<byte>)},
            {"UInt16", typeof(SimpleColumnType<ushort>)},
            {"UInt32", typeof(SimpleColumnType<uint>)},
            {"UInt64", typeof(SimpleColumnType<ulong>)},
            {"Int8", typeof(SimpleColumnType<sbyte>)},
            {"Int16", typeof(SimpleColumnType<short>)},
            {"Int32", typeof(SimpleColumnType<int>)},
            {"Int64", typeof(SimpleColumnType<long>)},
            {"Float32", typeof(SimpleColumnType<float>)},
            {"Float64", typeof(SimpleColumnType<double>)},
            {"Single", typeof(SimpleColumnType<float>)},
            {"Double", typeof(SimpleColumnType<double>)},
            {"Date", typeof(DateColumnType)},
            {"DateTime", typeof(DateTimeColumnType)},
            {"DateTime64", typeof(DateTime64ColumnType)},
            {"String", typeof(StringColumnType)},
            {"Null", typeof(NullColumnType)},
            {GuidColumnType.UuidColumnTypeName, typeof(GuidColumnType)},
            {"Nothing", typeof(NullColumnType)}
        };

        private static readonly Regex FixedStringRegex = new Regex(@"^FixedString\s*\(\s*(?<len>\d+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex NestedRegex = new Regex(@"^(?<outer>\w+)\s*\(\s*(?<inner>.+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        private static readonly Regex DecimalRegex = new Regex(@"^Decimal(((?<dlen>(32|64|128|256))\s*\()|\s*\(\s*(?<len>\d+)\s*,)\s*(?<prec>\d+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex DateTime64Regex = new Regex(@"^DateTime64\s*\(\s*(?<prec>\d+)\s*(,\s*'(?<tz>([^']|(\\'))*)'\s*)?\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex DateTimeRegex = new Regex(@"^DateTime\s*\('[^']|(\\')*'\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        public virtual bool IsNullable => false;
        public abstract int Rows { get; }

        internal abstract Type CLRType { get; }
        internal abstract void Read(ProtocolFormatter formatter, int rows);

        public static ColumnType Create(string name) {
            if (CaseInsensitiveTypeAliases.TryGetValue(name, out var alias)) name = alias;
            if (Types.ContainsKey(name))
                return (ColumnType) Activator.CreateInstance(Types[name]);
            var m = FixedStringRegex.Match(name);
            if (m.Success) return new FixedStringColumnType(uint.Parse(m.Groups["len"].Value));
            m = DecimalRegex.Match(name);
            if (m.Success) {
                uint len;
                if (m.Groups["dlen"].Success)
                    switch (m.Groups["dlen"].Value) {
                        case "32":
                            len = 9;
                            break;
                        case "64":
                            len = 18;
                            break;
                        case "128":
                            len = 38;
                            break;
                        case "256":
                            len = 76;
                            break;
                        default:
                            throw new ClickHouseException($"Invalid Decimal bit-length {m.Groups["dlen"].Value}");
                    }
                else
                    len = uint.Parse(m.Groups["len"].Value);

                return new DecimalColumnType(len, uint.Parse(m.Groups["prec"].Value));
            }

            m = DateTime64Regex.Match(name);
            if (m.Success) return new DateTime64ColumnType(int.Parse(m.Groups["prec"].Value), ProtocolFormatter.UnescapeStringValue(m.Groups["tz"].Value));
            m = DateTimeRegex.Match(name);
            if (m.Success) return new DateTimeColumnType();
            m = NestedRegex.Match(name);
            if (m.Success)
                switch (m.Groups["outer"].Value) {
                    case "Nullable":
                        return new NullableColumnType(Create(m.Groups["inner"].Value));
                    case "LowCardinality":
                        return new LowCardinalityColumnType(Create(m.Groups["inner"].Value));
                    case "Array":
                        if (m.Groups["inner"].Value == "Null")
                            return new ArrayColumnType(new NullableColumnType(new SimpleColumnType<byte>()));
                        return new ArrayColumnType(Create(m.Groups["inner"].Value));
                    case "AggregateFunction":
                        //See ClickHouse\dbms\src\DataTypes\DataTypeFactory.cpp:128
                        throw new NotSupportedException($"AggregateFunction({m.Groups["inner"].Value}) column type is not supported");
                    case "Nested":
                        //See ClickHouse\dbms\src\DataTypes\DataTypeFactory.cpp:189
                        throw new NotSupportedException($"Nested({m.Groups["inner"].Value}) column type is not supported");
                    case "Tuple": {
                        var parser = new ATG.IdentList.Parser(new Scanner(new MemoryStream(Encoding.UTF8.GetBytes(m.Groups["inner"].Value))));
                        parser.Parse();
                        if (parser.errors != null && parser.errors.count > 0)
                            throw new FormatException($"Bad enum description: {m.Groups["inner"].Value}.");
                        return new TupleColumnType(parser.result.Select(x => Create(x)));
                    }
                    case "Enum8":
                    case "Enum16": {
                        var parser = new ATG.Enums.Parser(new ATG.Enums.Scanner(new MemoryStream(Encoding.UTF8.GetBytes(m.Groups["inner"].Value))));
                        parser.Parse();
                        if (parser.errors != null && parser.errors.count > 0)
                            throw new FormatException($"Bad enum description: {m.Groups["inner"].Value}.");
                        return new EnumColumnType(m.Groups["outer"].Value == "Enum8" ? 8 : 16, parser.result);
                    }
                }

            throw new NotSupportedException($"Unknown column type {name}");
        }

        //public abstract void ValueFromConst(string value, Parser.ConstType typeHint);
        public abstract void ValueFromConst(Parser.ValueType val);
        public abstract string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent);
        public abstract void Write(ProtocolFormatter formatter, int rows);

        public abstract void ValueFromParam(ClickHouseParameter parameter);
        public abstract object Value(int currentRow);
        public abstract long IntValue(int currentRow);
        public abstract void ValuesFromConst(IEnumerable objects);

        public virtual void NullableValuesFromConst(IEnumerable objects) => ValuesFromConst(objects);

        private class CaseInsensitiveComparer : IEqualityComparer<string> {
            public bool Equals(string x, string y) => string.Equals(x, y, StringComparison.OrdinalIgnoreCase);

            public int GetHashCode(string obj) => obj.ToLowerInvariant().GetHashCode();
        }
    }
}

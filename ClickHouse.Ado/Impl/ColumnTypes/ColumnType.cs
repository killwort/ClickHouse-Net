using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal abstract class ColumnType
    {
        internal abstract void Read(ProtocolFormatter formatter, int rows);
        public virtual bool IsNullable => false;
        public abstract int Rows { get; }

        private static Dictionary<string, Type> Types = new Dictionary<string, Type>
        {
            {"UInt8", typeof(SimpleColumnType<byte>)},
            {"UInt16", typeof(SimpleColumnType<UInt16>)},
            {"UInt32", typeof(SimpleColumnType<UInt32>)},
            {"UInt64", typeof(SimpleColumnType<UInt64>)},
            {"Int8", typeof(SimpleColumnType<sbyte>)},
            {"Int16", typeof(SimpleColumnType<Int16>)},
            {"Int32", typeof(SimpleColumnType<Int32>)},
            {"Int64", typeof(SimpleColumnType<Int64>)},
            {"Float32", typeof(SimpleColumnType<float>)},
            {"Float64", typeof(SimpleColumnType<double>)},
            {"Single", typeof(SimpleColumnType<float>)},
            {"Double", typeof(SimpleColumnType<double>)},
            {"Date", typeof(DateColumnType)},
            {"DateTime", typeof(DateTimeColumnType)},
            {"String", typeof(StringColumnType)},
            {"Null", typeof(NullColumnType)},
            {GuidColumnType.UuidColumnTypeName, typeof(GuidColumnType)},
            {"Nothing", typeof(NullColumnType)}
        };

        internal abstract Type CLRType { get; }

        private static readonly Regex FixedStringRegex = new Regex(@"^FixedString\s*\(\s*(?<len>\d+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex NestedRegex = new Regex(@"^(?<outer>\w+)\s*\(\s*(?<inner>.+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        private static readonly Regex DecimalRegex = new Regex(@"^Decimal(((?<dlen>(32|64|128))\s*\()|\s*\(\s*(?<len>\d+)\s*,)\s*(?<prec>\d+)\s*\)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static ColumnType Create(string name)
        {
            if (Types.ContainsKey(name))
                return (ColumnType)Activator.CreateInstance(Types[name]);

            var m = FixedStringRegex.Match(name);
            if (m.Success)
            {
                return new FixedStringColumnType(uint.Parse(m.Groups["len"].Value));
            }
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
                        default:
                            throw new ClickHouseException($"Invalid Decimal bit-length {m.Groups["dlen"].Value}");
                    }
                else
                    len = uint.Parse(m.Groups["len"].Value);
                return new DecimalColumnType(len,uint.Parse(m.Groups["prec"].Value));
            }
            m = NestedRegex.Match(name);
            if (m.Success)
            {

                switch (m.Groups["outer"].Value)
                {
                    case "Nullable":
                        return new NullableColumnType(Create(m.Groups["inner"].Value));
                    case "Array":
                        if (m.Groups["inner"].Value == "Null")
                            return new ArrayColumnType(new NullableColumnType(new SimpleColumnType<byte>()));
                        return new ArrayColumnType(Create(m.Groups["inner"].Value));
                    case "AggregateFunction":
                        //See ClickHouse\dbms\src\DataTypes\DataTypeFactory.cpp:128
                        throw new NotImplementedException($"AggregateFunction({m.Groups["inner"].Value}) column type is not supported");
                    case "Nested":
                        //See ClickHouse\dbms\src\DataTypes\DataTypeFactory.cpp:189
                        throw new NotImplementedException($"Nested({m.Groups["inner"].Value}) column type is not supported");
                    case "Tuple":
                        {
                            var parser = new ATG.IdentList.Parser(new ATG.IdentList.Scanner(new MemoryStream(Encoding.UTF8.GetBytes(m.Groups["inner"].Value))));
                            parser.Parse();
                            if (parser.errors != null && parser.errors.count > 0)
                                throw new FormatException($"Bad enum description: {m.Groups["inner"].Value}.");
                            return new TupleColumnType(parser.result.Select(x => Create(x)));
                        }
                    case "Enum8":
                    case "Enum16":
                        {
                            var parser = new ATG.Enums.Parser(new ATG.Enums.Scanner(new MemoryStream(Encoding.UTF8.GetBytes(m.Groups["inner"].Value))));
                            parser.Parse();
                            if (parser.errors != null && parser.errors.count > 0)
                                throw new FormatException($"Bad enum description: {m.Groups["inner"].Value}.");
                            return new EnumColumnType(m.Groups["outer"].Value == "Enum8" ? 8 : 16, parser.result);
                        }
                }
            }
            throw new NotSupportedException($"Unknown column type {name}");
        }

        //public abstract void ValueFromConst(string value, Parser.ConstType typeHint);
        public abstract void ValueFromConst(Parser.ValueType val);
        public abstract string AsClickHouseType();
        public abstract void Write(ProtocolFormatter formatter, int rows);

        public abstract void ValueFromParam(ClickHouseParameter parameter);
        public abstract object Value(int currentRow);
        public abstract long IntValue(int currentRow);
        public abstract void ValuesFromConst(IEnumerable objects);

        public virtual void NullableValuesFromConst(IEnumerable objects)
        {
            ValuesFromConst(objects);
        }
    }
}
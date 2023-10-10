using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ClickHouse.Ado.Impl.ATG.Insert; 

internal class Parser {
    public enum ConstType {
        String,
        Number,
        Parameter,
        Array
    }

    public const int _EOF = 0;
    public const int _ident = 1;
    public const int _identBackquoted = 2;
    public const int _identQuoted = 3;
    public const int _stringValue = 4;
    public const int _numValue = 5;
    public const int _insert = 6;
    public const int _values = 7;
    public const int _into = 8;
    public const int maxT = 18;

    private const bool _T = true;
    private const bool _x = false;
    private const int minErrDist = 2;

    private static readonly bool[,] set = {
        {
            _T,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x,
            _x
        }
    };

    private int errDist = minErrDist;
    public Errors errors;
    internal IEnumerable<string> fieldList;
    public Token la; // lookahead token
    internal string oneParam, tableName;

    public Scanner scanner;

    public Token t; // last recognized token
    internal IEnumerable<ValueType> valueList;

    public Parser(Scanner scanner) {
        this.scanner = scanner;
        errors = new Errors();
    }

    private void SynErr(int n) {
        if (errDist >= minErrDist) errors.SynErr(la.line, la.col, n);
        errDist = 0;
    }

    public void SemErr(string msg) {
        if (errDist >= minErrDist) errors.SemErr(t.line, t.col, msg);
        errDist = 0;
    }

    private void Get() {
        for (;;) {
            t = la;
            la = scanner.Scan();
            if (la.kind <= maxT) {
                ++errDist;
                break;
            }

            la = t;
        }
    }

    private void Expect(int n) {
        if (la.kind == n) Get();
        else SynErr(n);
    }

    private bool StartOf(int s) => set[s, la.kind];

    private void ExpectWeak(int n, int follow) {
        if (la.kind == n) {
            Get();
        } else {
            SynErr(n);
            while (!StartOf(follow)) Get();
        }
    }

    private bool WeakSeparator(int n, int syFol, int repFol) {
        var kind = la.kind;
        if (kind == n) {
            Get();
            return true;
        }

        if (StartOf(repFol)) {
            return false;
        }

        SynErr(n);
        while (!(set[syFol, kind] || set[repFol, kind] || set[0, kind])) {
            Get();
            kind = la.kind;
        }

        return StartOf(syFol);
    }

    private void Identifier(out string name) {
        name = null;
        if (la.kind == 1) {
            Get();
            name = t.val;
        } else if (la.kind == 2) {
            Get();
            name = t.val;
        } else if (la.kind == 3) {
            Get();
            name = t.val;
        } else {
            SynErr(19);
        }
    }

    private void Field(out string name) {
        name = null;
        string prefix = "", suffix = "";
        Identifier(out prefix);
        if (la.kind == 9) {
            Get();
            Identifier(out suffix);
            suffix = "." + suffix;
        }

        name = prefix + suffix;
    }

    private void FieldList(out IEnumerable<string> elements) {
        var rv = new List<string>();
        elements = rv;
        string elem;
        IEnumerable<string> inner;
        Field(out elem);
        rv.Add(elem);
        if (la.kind == 10) {
            Get();
            FieldList(out inner);
            rv.AddRange(inner);
        }
    }

    private void Parameter(out string name) {
        if (la.kind == 11)
            Get();
        else if (la.kind == 12)
            Get();
        else SynErr(20);
        Expect(1);
        name = t.val;
    }

    private void Value(out ValueType val) {
        val = null;
        string paramName = null;
        IEnumerable<ValueType> inner;
        if (la.kind == 4) {
            Get();
            val = new ValueType {
                StringValue = t.val,
                TypeHint = ConstType.String
            };
        } else if (la.kind == 11 || la.kind == 12) {
            Parameter(out paramName);
            val = new ValueType {
                StringValue = paramName,
                TypeHint = ConstType.Parameter
            };
        } else if (la.kind == 5) {
            Get();
            val = new ValueType {
                StringValue = t.val,
                TypeHint = ConstType.Number
            };
        } else if (la.kind == 13) {
            Get();
            ValueList(out inner);
            val = new ValueType {
                ArrayValue = inner.ToArray(),
                TypeHint = ConstType.Array
            };
            Expect(14);
        } else {
            SynErr(21);
        }
    }

    private void ValueList(out IEnumerable<ValueType> elements) {
        var rv = new List<ValueType>();
        elements = rv;
        ValueType elem;
        IEnumerable<ValueType> inner;
        Value(out elem);
        rv.Add(elem);
        if (la.kind == 10) {
            Get();
            ValueList(out inner);
            rv.AddRange(inner);
        }
    }

    private void Insert() {
        Expect(6);
        Expect(8);
        Field(out tableName);
        if (la.kind == 15) {
            Get();
            FieldList(out fieldList);
            Expect(16);
        }

        Expect(7);
        if (la.kind == 11 || la.kind == 12) {
            Parameter(out oneParam);
        } else if (la.kind == 15) {
            Get();
            ValueList(out valueList);
            Expect(16);
        } else {
            SynErr(22);
        }

        while (la.kind == 17) Get();
    }

    public void Parse() {
        la = new Token();
        la.val = "";
        Get();
        Insert();
        Expect(0);
    }

    public class ValueType {
        public ValueType[] ArrayValue;
        public string StringValue;
        public ConstType TypeHint;
    }
} // end Parser

internal class Errors {
    public int count; // number of errors detected
    public string errMsgFormat = "-- line {0} col {1}: {2}"; // 0=line, 1=column, 2=text
    public TextWriter errorStream = Console.Out; // error messages go to this stream

    public virtual void SynErr(int line, int col, int n) {
        string s;
        switch (n) {
            case 0:
                s = "EOF expected";
                break;
            case 1:
                s = "ident expected";
                break;
            case 2:
                s = "identBackquoted expected";
                break;
            case 3:
                s = "identQuoted expected";
                break;
            case 4:
                s = "stringValue expected";
                break;
            case 5:
                s = "numValue expected";
                break;
            case 6:
                s = "insert expected";
                break;
            case 7:
                s = "values expected";
                break;
            case 8:
                s = "into expected";
                break;
            case 9:
                s = "\".\" expected";
                break;
            case 10:
                s = "\",\" expected";
                break;
            case 11:
                s = "\"@\" expected";
                break;
            case 12:
                s = "\":\" expected";
                break;
            case 13:
                s = "\"[\" expected";
                break;
            case 14:
                s = "\"]\" expected";
                break;
            case 15:
                s = "\"(\" expected";
                break;
            case 16:
                s = "\")\" expected";
                break;
            case 17:
                s = "\";\" expected";
                break;
            case 18:
                s = "??? expected";
                break;
            case 19:
                s = "invalid Identifier";
                break;
            case 20:
                s = "invalid Parameter";
                break;
            case 21:
                s = "invalid Value";
                break;
            case 22:
                s = "invalid Insert";
                break;

            default:
                s = "error " + n;
                break;
        }

        errorStream.WriteLine(errMsgFormat, line, col, s);
        count++;
    }

    public virtual void SemErr(int line, int col, string s) {
        errorStream.WriteLine(errMsgFormat, line, col, s);
        count++;
    }

    public virtual void SemErr(string s) {
        errorStream.WriteLine(s);
        count++;
    }

    public virtual void Warning(int line, int col, string s) => errorStream.WriteLine(errMsgFormat, line, col, s);

    public virtual void Warning(string s) => errorStream.WriteLine(s);
} // Errors

internal class FatalError : Exception {
    public FatalError(string m) : base(m) { }
}
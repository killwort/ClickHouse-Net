using System;
using System.Collections.Generic;
using System.IO;

namespace ClickHouse.Ado.Impl.ATG.IdentList; 

internal class Parser {
    public const int _EOF = 0;
    public const int _ident = 1;
    public const int _identBackquoted = 2;
    public const int _identQuoted = 3;
    public const int maxT = 5;

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
            _x
        }
    };

    private int errDist = minErrDist;
    public Errors errors;
    public Token la; // lookahead token

    internal IEnumerable<string> result;

    public Scanner scanner;

    public Token t; // last recognized token

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

    private void Element(out string name) {
        name = null;
        if (la.kind == 1) {
            Get();
            name = t.val;
        } else if (la.kind == 2) {
            Get();
            name = t.val.Substring(1, t.val.Length - 2);
        } else if (la.kind == 3) {
            Get();
            name = t.val.Substring(1, t.val.Length - 2);
        } else {
            SynErr(6);
        }
    }

    private void ElementList(out IEnumerable<string> elements) {
        var rv = new List<string>();
        elements = rv;
        string elem;
        IEnumerable<string> inner;
        Element(out elem);
        rv.Add(elem);
        if (la.kind == 4) {
            Get();
            ElementList(out inner);
            rv.AddRange(inner);
        }
    }

    private void IdentList() {
        IEnumerable<string> elems;
        ElementList(out elems);
        result = elems;
    }

    public void Parse() {
        la = new Token();
        la.val = "";
        Get();
        IdentList();
        Expect(0);
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
                s = "\",\" expected";
                break;
            case 5:
                s = "??? expected";
                break;
            case 6:
                s = "invalid Element";
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
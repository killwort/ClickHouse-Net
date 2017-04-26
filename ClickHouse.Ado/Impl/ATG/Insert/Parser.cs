using System.Collections.Generic;


using System;

namespace ClickHouse.Ado.Impl.ATG.Insert {



internal class Parser {
	public const int _EOF = 0;
	public const int _ident = 1;
	public const int _stringValue = 2;
	public const int _numValue = 3;
	public const int _insert = 4;
	public const int _values = 5;
	public const int _into = 6;
	public const int maxT = 13;

	const bool T = true;
	const bool x = false;
	const int minErrDist = 2;
	
	public Scanner scanner;
	public Errors  errors;

	public Token t;    // last recognized token
	public Token la;   // lookahead token
	int errDist = minErrDist;

public enum ConstType{String,Number,Parameter};
internal IEnumerable<string> fieldList;
internal IEnumerable<Tuple<string,ConstType> > valueList;
internal string oneParam,tableName;


	public Parser(Scanner scanner) {
		this.scanner = scanner;
		errors = new Errors();
	}

	void SynErr (int n) {
		if (errDist >= minErrDist) errors.SynErr(la.line, la.col, n);
		errDist = 0;
	}

	public void SemErr (string msg) {
		if (errDist >= minErrDist) errors.SemErr(t.line, t.col, msg);
		errDist = 0;
	}
	
	void Get () {
		for (;;) {
			t = la;
			la = scanner.Scan();
			if (la.kind <= maxT) { ++errDist; break; }

			la = t;
		}
	}
	
	void Expect (int n) {
		if (la.kind==n) Get(); else { SynErr(n); }
	}
	
	bool StartOf (int s) {
		return set[s, la.kind];
	}
	
	void ExpectWeak (int n, int follow) {
		if (la.kind == n) Get();
		else {
			SynErr(n);
			while (!StartOf(follow)) Get();
		}
	}


	bool WeakSeparator(int n, int syFol, int repFol) {
		int kind = la.kind;
		if (kind == n) {Get(); return true;}
		else if (StartOf(repFol)) {return false;}
		else {
			SynErr(n);
			while (!(set[syFol, kind] || set[repFol, kind] || set[0, kind])) {
				Get();
				kind = la.kind;
			}
			return StartOf(syFol);
		}
	}

	
	void Field(out string name ) {
		Expect(1);
		name=t.val; 
	}

	void FieldList(out IEnumerable<string> elements ) {
		var rv=new List<string>(); elements=rv; string elem; IEnumerable<string> inner; 
		Field(out elem);
		rv.Add(elem); 
		if (la.kind == 7) {
			Get();
			FieldList(out inner);
			rv.AddRange(inner); 
		}
	}

	void Parameter(out string name ) {
		if (la.kind == 8) {
			Get();
		} else if (la.kind == 9) {
			Get();
		} else SynErr(14);
		Expect(1);
		name=t.val; 
	}

	void Value(out Tuple<string,ConstType> val ) {
		val = null; string paramName=null; 
		if (la.kind == 2) {
			Get();
			val=Tuple.Create(t.val,ConstType.String); 
		} else if (la.kind == 8 || la.kind == 9) {
			Parameter(out paramName);
			val=Tuple.Create(paramName,ConstType.Parameter); 
		} else if (la.kind == 3) {
			Get();
			val=Tuple.Create(t.val,ConstType.Number); 
		} else SynErr(15);
	}

	void ValueList(out IEnumerable<Tuple<string,ConstType> > elements ) {
		var rv=new List<Tuple<string,ConstType> >(); elements=rv; Tuple<string,ConstType> elem; IEnumerable<Tuple<string,ConstType> > inner; 
		Value(out elem);
		rv.Add(elem); 
		if (la.kind == 7) {
			Get();
			ValueList(out inner);
			rv.AddRange(inner); 
		}
	}

	void Insert() {
		
		Expect(4);
		Expect(6);
		Field(out tableName);
		Expect(10);
		FieldList(out fieldList);
		Expect(11);
		Expect(5);
		if (la.kind == 8 || la.kind == 9) {
			Parameter(out oneParam);
		} else if (la.kind == 10) {
			Get();
			ValueList(out valueList);
			Expect(11);
		} else SynErr(16);
		while (la.kind == 12) {
			Get();
		}
	}



	public void Parse() {
		la = new Token();
		la.val = "";		
		Get();
		Insert();
		Expect(0);

	}
	
	static readonly bool[,] set = {
		{T,x,x,x, x,x,x,x, x,x,x,x, x,x,x}

	};
} // end Parser


internal class Errors {
	public int count = 0;                                    // number of errors detected
	public System.IO.TextWriter errorStream = Console.Out;   // error messages go to this stream
	public string errMsgFormat = "-- line {0} col {1}: {2}"; // 0=line, 1=column, 2=text

	public virtual void SynErr (int line, int col, int n) {
		string s;
		switch (n) {
			case 0: s = "EOF expected"; break;
			case 1: s = "ident expected"; break;
			case 2: s = "stringValue expected"; break;
			case 3: s = "numValue expected"; break;
			case 4: s = "insert expected"; break;
			case 5: s = "values expected"; break;
			case 6: s = "into expected"; break;
			case 7: s = "\",\" expected"; break;
			case 8: s = "\"@\" expected"; break;
			case 9: s = "\":\" expected"; break;
			case 10: s = "\"(\" expected"; break;
			case 11: s = "\")\" expected"; break;
			case 12: s = "\";\" expected"; break;
			case 13: s = "??? expected"; break;
			case 14: s = "invalid Parameter"; break;
			case 15: s = "invalid Value"; break;
			case 16: s = "invalid Insert"; break;

			default: s = "error " + n; break;
		}
		errorStream.WriteLine(errMsgFormat, line, col, s);
		count++;
	}

	public virtual void SemErr (int line, int col, string s) {
		errorStream.WriteLine(errMsgFormat, line, col, s);
		count++;
	}
	
	public virtual void SemErr (string s) {
		errorStream.WriteLine(s);
		count++;
	}
	
	public virtual void Warning (int line, int col, string s) {
		errorStream.WriteLine(errMsgFormat, line, col, s);
	}
	
	public virtual void Warning(string s) {
		errorStream.WriteLine(s);
	}
} // Errors


internal class FatalError: Exception {
	public FatalError(string m): base(m) {}
}
}
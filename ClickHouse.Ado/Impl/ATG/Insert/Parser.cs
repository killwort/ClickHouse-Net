using System.Collections.Generic;
using System.Linq;


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
	public const int maxT = 15;

	const bool _T = true;
	const bool _x = false;
	const int minErrDist = 2;
	
	public Scanner scanner;
	public Errors  errors;

	public Token t;    // last recognized token
	public Token la;   // lookahead token
	int errDist = minErrDist;

public enum ConstType{String,Number,Parameter,Array};
public class ValueType{
	public string StringValue;
	public ValueType[] ArrayValue;
	public ConstType TypeHint;
}
internal IEnumerable<string> fieldList;
internal IEnumerable<ValueType> valueList;
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
		} else SynErr(16);
		Expect(1);
		name=t.val; 
	}

	void Value(out ValueType val ) {
		val = null; string paramName=null; IEnumerable<ValueType > inner; 
		if (la.kind == 2) {
			Get();
			val=new ValueType{StringValue=t.val,TypeHint=ConstType.String}; 
		} else if (la.kind == 8 || la.kind == 9) {
			Parameter(out paramName);
			val=new ValueType{StringValue=paramName,TypeHint=ConstType.Parameter}; 
		} else if (la.kind == 3) {
			Get();
			val=new ValueType{StringValue=t.val,TypeHint=ConstType.Number}; 
		} else if (la.kind == 10) {
			Get();
			ValueList(out inner);
			val=new ValueType{ArrayValue=inner.ToArray(), TypeHint=ConstType.Array}; 
			Expect(11);
		} else SynErr(17);
	}

	void ValueList(out IEnumerable<ValueType > elements ) {
		var rv=new List<ValueType >(); elements=rv; ValueType elem; IEnumerable<ValueType > inner; 
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
		if (la.kind == 12) {
			Get();
			FieldList(out fieldList);
			Expect(13);
		}
		Expect(5);
		if (la.kind == 8 || la.kind == 9) {
			Parameter(out oneParam);
		} else if (la.kind == 12) {
			Get();
			ValueList(out valueList);
			Expect(13);
		} else SynErr(18);
		while (la.kind == 14) {
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
		{_T,_x,_x,_x, _x,_x,_x,_x, _x,_x,_x,_x, _x,_x,_x,_x, _x}

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
			case 10: s = "\"[\" expected"; break;
			case 11: s = "\"]\" expected"; break;
			case 12: s = "\"(\" expected"; break;
			case 13: s = "\")\" expected"; break;
			case 14: s = "\";\" expected"; break;
			case 15: s = "??? expected"; break;
			case 16: s = "invalid Parameter"; break;
			case 17: s = "invalid Value"; break;
			case 18: s = "invalid Insert"; break;

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
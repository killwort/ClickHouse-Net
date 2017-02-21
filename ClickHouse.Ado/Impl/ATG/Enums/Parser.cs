using System.Collections.Generic;


using System;

namespace ClickHouse.Ado.Impl.ATG.Enums {



public class Parser {
	public const int _EOF = 0;
	public const int _ident = 1;
	public const int _value = 2;
	public const int maxT = 5;

	const bool T = true;
	const bool x = false;
	const int minErrDist = 2;
	
	public Scanner scanner;
	public Errors  errors;

	public Token t;    // last recognized token
	public Token la;   // lookahead token
	int errDist = minErrDist;

internal IEnumerable<Tuple<string,int> > result;


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

	
	void Element(out Tuple<string,int> name ) {
		name = null; string n; 
		Expect(1);
		n=t.val; 
		Expect(3);
		Expect(2);
		name=Tuple.Create(n,int.Parse(t.val)); 
	}

	void ElementList(out IEnumerable<Tuple<string,int> > elements ) {
		var rv=new List<Tuple<string,int> >(); elements=rv; Tuple<string,int> elem; IEnumerable<Tuple<string,int> > inner; 
		Element(out elem);
		rv.Add(elem); 
		if (la.kind == 4) {
			Get();
			ElementList(out inner);
			rv.AddRange(inner); 
		}
	}

	void Enums() {
		IEnumerable<Tuple<string,int> > elems; 
		ElementList(out elems);
		result = elems; 
	}



	public void Parse() {
		la = new Token();
		la.val = "";		
		Get();
		Enums();
		Expect(0);

	}
	
	static readonly bool[,] set = {
		{T,x,x,x, x,x,x}

	};
} // end Parser


public class Errors {
	public int count = 0;                                    // number of errors detected
	public System.IO.TextWriter errorStream = Console.Out;   // error messages go to this stream
	public string errMsgFormat = "-- line {0} col {1}: {2}"; // 0=line, 1=column, 2=text

	public virtual void SynErr (int line, int col, int n) {
		string s;
		switch (n) {
			case 0: s = "EOF expected"; break;
			case 1: s = "ident expected"; break;
			case 2: s = "value expected"; break;
			case 3: s = "\"=\" expected"; break;
			case 4: s = "\",\" expected"; break;
			case 5: s = "??? expected"; break;

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


public class FatalError: Exception {
	public FatalError(string m): base(m) {}
}
}
using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace Yavit.StellaDB
{
	[Serializable]
	public class InvalidQueryException: Exception
	{
		public InvalidQueryException(string text):
		base(text) { }
	}
	[Serializable]
	public class InvalidQuerySemanticsException: InvalidQueryException
	{
		public InvalidQuerySemanticsException(string query, int index):
		this(query, index, "") {}
		public InvalidQuerySemanticsException(string query, int index, string text): 
		base(string.Format("Query text '{0}' has a error around position {1}. {2}",
			query, index, text)) {}
	}
	[Serializable]
	public class InvalidQuerySyntaxException: InvalidQueryException
	{
		public InvalidQuerySyntaxException(string query, int index):
		this(query, index, "") {}
		public InvalidQuerySyntaxException(string query, int index, string text): 
		base(string.Format("Query text '{0}' has a syntax error around position {1}. {2}",
			query, index, text)) {}
	}

	public partial class PreparedTextQuery
	{
		enum TokenType {
			End,
			Identifier,
			Parameter,
			Constant,
			Operator
		}
		sealed class Tokenizer
		{
			readonly string text;
			int index = 0;
			TokenType token;
			string tokenText;
			object tokenValue;
			StringBuilder stringBuilder = new StringBuilder ();
			int lastTokenIndex;

			public Tokenizer(string text)
			{
				this.text = text;
				Read();
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			static bool IsWhitespace(char c)
			{
				return char.IsWhiteSpace (c);
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			static bool IsIdentifierChar(char c)
			{
				return char.IsLetterOrDigit (c) || c == '_';
			}

			void SkipWhitespace()
			{
				while (index < text.Length && IsWhitespace (text [index]))
					++index;
			}

			public void Read()
			{
				SkipWhitespace();
				lastTokenIndex = index;
				if (index >= text.Length) {
					token = TokenType.End;
					return;
				}

				switch (text[index]) {
				case '`':
					ReadIdentifierQuoted ();
					break;
				case '\'':
					ReadCharLiteral ();
					break;
				case '"':
					ReadStringLiteral ();
					break;
				case '[':
					ReadParameter ();
					break;
				default:
					if (char.IsDigit (text [index])) {
						ReadNumericLiteral ();
					} else if (IsIdentifierChar (text [index])) {
						ReadIdentifier ();
					} else if (!ReadOperatorRegex()) {
						throw new InvalidQuerySyntaxException (text, index, 
							string.Format ("Invalid character '{0}'.", text [index]));
					}
					break;
				}

			}


			static readonly Regex operatorRegex = new Regex(
				"([=<>!]=|&&|\\|\\||[+-/*|&!~?:#<>])", 
				RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

			bool ReadOperatorRegex()
			{
				var match = operatorRegex.Match (text, index);
				if (!match.Success || match.Index > index) {
					return false;
				}

				token = TokenType.Operator;
				tokenText = match.Groups [0].Value;
				index += match.Groups [0].Length;
				return true;
			}

			static readonly Regex numRegex = new Regex(
				"([0-9]*(\\.[0-9])?(e[+-][0-9]+)?)", 
				RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

			void ReadNumericLiteral()
			{
				var match = numRegex.Match (text, index);
				if (!match.Success || match.Index > index) {
					throw new InvalidQuerySyntaxException(text, index, "Unrecognized numeric literal.");
				}

				var isFp = match.Groups [1].Success ||
					match.Groups [2].Success;

				token = TokenType.Constant;

				if (isFp) {
					tokenValue = double.Parse (match.Groups [0].Value);
				} else {
					long l;
					if (long.TryParse(match.Groups[0].Value, out l)) {
						tokenValue = l;
					} else {
						tokenValue = ulong.Parse (match.Groups [0].Value);
					}
				}

				index += match.Groups [0].Length;
			}

			void ReadParameter()
			{
				++index;
				token = TokenType.Parameter;
				ReadStringGeneric (']');
			}

			void ReadIdentifier()
			{
				var sb = stringBuilder;
				var text = this.text;
				var index = this.index;
				sb.Clear ();
				token = TokenType.Identifier;

				while (index < text.Length && IsIdentifierChar (text [index]))
					sb.Append (text [index++]);

				tokenText = sb.ToString ();
			}

			void ReadIdentifierQuoted()
			{
				++index;
				token = TokenType.Identifier;
				ReadStringGeneric ('`');
			}

			void ReadStringLiteral()
			{
				++index;
				token = TokenType.Constant;
				ReadStringGeneric ('"');
				tokenValue = tokenText;
			}

			void ReadCharLiteral()
			{
				var i = index;
				++index;
				token = TokenType.Constant;
				ReadStringGeneric ('\'');
				if (tokenText.Length != 1) {
					throw new InvalidQuerySyntaxException (text, i, "Character literal must contain exactly one character.");
				}
				tokenValue = tokenText[0];
			}

			void ReadStringGeneric(char closingChar)
			{
				var sb = stringBuilder;
				var text = this.text;
				var index = this.index;
				sb.Clear ();

				while (true) {
					if (index >= text.Length) {
						throw new InvalidQuerySyntaxException (text, this.index, "Quoted string is not closed.");
					}

					if (text [index] == '\\' && index < text.Length - 1) {
						switch (text[index + 1]) {
						case 'n':
							sb.Append ('\n');
							index += 2;
							continue;
						case 'r':
							sb.Append ('\r');
							index += 2;
							continue;
						case 't':
							sb.Append ('\t');
							index += 2;
							continue;
						default:
							sb.Append (text[index + 1]);
							index += 2;
							continue;
						}
					} else if (text[index] == closingChar) {
						break;
					}

					sb.Append (text [index]);
					++index;
				}

				this.index = index + 1;
				tokenText = sb.ToString ();
			}

			public TokenType CurrentTokenType
			{
				get { return token; }
			}
			public string CurrentTokenText
			{
				get { return tokenText; }
			}
			public object CurrentTokenConstantValue
			{
				get { return tokenValue; }
			}
			public int CurrentTokenIndex
			{
				get { return lastTokenIndex; }
			}
			public string Text
			{
				get { return text; }
			}
			public string CurrentTokenDisplayName
			{
				get {
					switch (CurrentTokenType) {
					case TokenType.End:
						return "end of query text";
					case TokenType.Identifier:
						return "`" + CurrentTokenText + "` (identifier)";
					case TokenType.Parameter:
						return "[" + CurrentTokenText + "] (parameter)";
					case TokenType.Constant:
						if (CurrentTokenConstantValue is string) {
							return "\"" + CurrentTokenConstantValue + "\"";
						} else {
							return CurrentTokenConstantValue.ToString();
						}
					case TokenType.Operator:
						return "operator " + CurrentTokenText;
					default:
						throw new InvalidOperationException ();
					}
				}
			}
		}

		sealed class ParsedQueryText
		{
			public Expression<Func<long, Ston.StonVariant, bool>> Predicate;
			public object[] Parameters;
			public string[] ParameterNames;
		}

		sealed class Parser
		{
			readonly Tokenizer tokenizer;

			readonly ParsedQueryText result = new ParsedQueryText();
			readonly ConstantExpression resultExpr;
			readonly ParameterExpression rowIdExpr = Expression.Parameter(typeof(long), "rowId");
			readonly ParameterExpression valueExpr = Expression.Parameter(typeof(Ston.StonVariant), "value");
			readonly Expression resultParametersArrayExpr;

			readonly List<string> parameterNames = new List<string> ();
			readonly Dictionary<string, int> parameterMap = new Dictionary<string, int> ();

			static readonly Type variantType = typeof(Ston.StonVariant);
			static readonly System.Reflection.MethodInfo variantCompareTo =
				variantType.GetMethod("CompareTo", new [] {typeof(object)});
			static readonly System.Reflection.MethodInfo variantGetItem =
				variantType.GetMethod("get_Item", new [] {typeof(string)});

			Parser(string text)
			{
				this.tokenizer = new Tokenizer(text);

				resultExpr = Expression.Constant (result, typeof(ParsedQueryText));
				resultParametersArrayExpr = Expression.MakeMemberAccess(
					resultExpr, typeof(ParsedQueryText).GetField("Parameters"));
			}

			static string GetTypeName(Type t)
			{
				if (t == typeof(Ston.StonVariant))
					return "a field value";
				else if (t == typeof(bool))
					return "a boolean value";
				else if (t == typeof(object))
					return "a parameter value";
				else if (t == typeof(string))
					return "a string constant";
				else
					return "a number";
			}

			Exception MakeUnexpectedToken(string expected)
			{
				return new InvalidQuerySyntaxException (tokenizer.Text, tokenizer.CurrentTokenIndex,
					string.Format ("Expected '{0}', but found '{1}'.", expected, tokenizer.CurrentTokenDisplayName));
			}

			Expression ParsePrimaryExpression()
			{
				Expression expr;
				switch (tokenizer.CurrentTokenType) {
				case TokenType.Identifier:
					expr = Expression.Call (valueExpr, variantGetItem, new [] {
						Expression.Constant(tokenizer.CurrentTokenText, typeof(string))
					});
					tokenizer.Read ();
					break;
				case TokenType.Parameter:
					string paramName = tokenizer.CurrentTokenText;
					tokenizer.Read ();
					int index;
					if (!parameterMap.TryGetValue(paramName, out index)) {
						index = parameterNames.Count;
						parameterNames.Add (paramName);
						parameterMap.Add (paramName, index);
					}
					expr = Expression.ArrayIndex (resultParametersArrayExpr,
						Expression.Constant (index, typeof(int)));
					break;
				case TokenType.Constant:
					expr = Expression.Constant (tokenizer.CurrentTokenConstantValue);
					tokenizer.Read ();
					break;
				case TokenType.Operator:
					if (tokenizer.CurrentTokenText == "#") {
						expr = rowIdExpr;
						tokenizer.Read ();
					} else {
						goto default;
					}
					break;
				default:
					throw MakeUnexpectedToken ("primary value");
				}
				return expr;
			}

			Expression ParseUnaryExpression()
			{
				if (tokenizer.CurrentTokenType == TokenType.Operator) {
					var text = tokenizer.CurrentTokenText;
					var i = tokenizer.CurrentTokenIndex;
					if (text == "!") {
						tokenizer.Read ();
						var expr = ParseUnaryExpression ();
						if (expr.Type != typeof(bool)) {
							throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
								string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(expr.Type)));
						}
						return Expression.Not (expr);
					}
				}

				return ParsePrimaryExpression ();
			}

			enum ComparsionType
			{
				Equality,
				Inequality,
				LessThan,
				LessThanOrEqual,
				GreaterThan,
				GreaterThanOrEqual
			}

			struct ComparsionItem
			{
				public Expression Expression;
				public ComparsionType Type;
				public int Index;
				public string QueryText;

				Expression MakeNumericComparsionExpression(Expression left, Expression right)
				{
					switch (Type) {
					case ComparsionType.Equality:
						return Expression.Equal (left, right);
					case ComparsionType.Inequality:
						return Expression.NotEqual (left, right);
					case ComparsionType.LessThan:
						return Expression.LessThan (left, right);
					case ComparsionType.LessThanOrEqual:
						return Expression.LessThanOrEqual (left, right);
					case ComparsionType.GreaterThan:
						return Expression.GreaterThan (left, right);
					case ComparsionType.GreaterThanOrEqual:
						return Expression.GreaterThanOrEqual (left, right);
					default:
						throw new InvalidOperationException ();
					}
				}

				public Expression MakeExpression(Expression left)
				{
					var leftType = left.Type;
					var rightType = this.Expression.Type;
					if (leftType == typeof(long) ||
						leftType == typeof(ulong) || 
						leftType == typeof(double)) {
						if (rightType == leftType) {
							return MakeNumericComparsionExpression (left, Expression);
						} else {
							var comparer = typeof(Utils.NumberComparatorExtension);
							var compare = comparer.GetMethod ("CompareTo2",
								              new [] { leftType, rightType });
							var compareExpr = Expression.Call (compare, left, Expression);
							return MakeNumericComparsionExpression (compareExpr,
								Expression.Constant(0, typeof(int)));
						}
					}

					if (leftType == typeof(Ston.StonVariant)) {
						var compareExpr = Expression.Call (left, variantCompareTo, Expression);
						return MakeNumericComparsionExpression (compareExpr,
							Expression.Constant(0, typeof(int)));
					}

					if (rightType == typeof(Ston.StonVariant)) {
						var compareExpr = Expression.Call (left, variantCompareTo, Expression);
						return MakeNumericComparsionExpression (Expression.Constant(0, typeof(int)),
							compareExpr);
					}

					throw new InvalidQuerySemanticsException (QueryText, Index,
						string.Format("Unsupported comparsion between '{0}' and '{1}'.",
							GetTypeName(leftType), GetTypeName(rightType)));
				}
			}

			Expression ParseComparsionExpression()
			{
				var expr = ParseUnaryExpression ();

				List<ComparsionItem> items = null;

				while (true) {
					if (tokenizer.CurrentTokenType == TokenType.Operator) {
						ComparsionType? typeOrNull = null;
						var i = tokenizer.CurrentTokenIndex;
						switch (tokenizer.CurrentTokenText) {
						case "==":
							typeOrNull = ComparsionType.Equality;
							break;
						case "!=":
							typeOrNull = ComparsionType.Inequality;
							break;
						case "<":
							typeOrNull = ComparsionType.LessThan;
							break;
						case ">":
							typeOrNull = ComparsionType.GreaterThan;
							break;
						case "<=":
							typeOrNull = ComparsionType.LessThanOrEqual;
							break;
						case ">=":
							typeOrNull = ComparsionType.GreaterThanOrEqual;
							break;
						}
						if (typeOrNull == null) {
							break;
						}
						var type = typeOrNull.Value;
						if (items == null) {
							items = new List<ComparsionItem> ();
						}
						tokenizer.Read ();

						var otherExpr = ParseUnaryExpression ();
						items.Add (new ComparsionItem () {
							Type = type,
							Expression = otherExpr,
							Index = i,
							QueryText = tokenizer.Text
						});

					} else {
						break;
					}
				}

				if (items != null) {
					expr = items [0].MakeExpression (expr);
					for (int i = 1; i < items.Count; ++i) {
						expr = Expression.AndAlso (expr, items[i].MakeExpression(items[i - 1].Expression));
					}
				}

				return expr;
			}

			Expression ParseLogicalAndExpression()
			{
				var expr = ParseComparsionExpression ();

				while (true) {
					if (tokenizer.CurrentTokenType == TokenType.Operator &&
						tokenizer.CurrentTokenText == "&&") {
						var i = tokenizer.CurrentTokenIndex;
						tokenizer.Read ();

						if (expr.Type != typeof(bool)) {
							throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
								string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(expr.Type)));
						}

						var otherExpr = ParseComparsionExpression ();
						if (otherExpr.Type != typeof(bool)) {
							throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
								string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(otherExpr.Type)));
						}

						expr = Expression.AndAlso (expr, otherExpr);
					} else {
						break;
					}
				}
				return expr;
			}

			Expression ParseLogicalOrExpression()
			{
				var expr = ParseLogicalAndExpression ();

				while (true) {
					if (tokenizer.CurrentTokenType == TokenType.Operator &&
						tokenizer.CurrentTokenText == "||") {
						var i = tokenizer.CurrentTokenIndex;
						tokenizer.Read ();

						if (expr.Type != typeof(bool)) {
							throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
								string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(expr.Type)));
						}

						var otherExpr = ParseLogicalAndExpression ();
						if (otherExpr.Type != typeof(bool)) {
							throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
								string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(otherExpr.Type)));
						}

						expr = Expression.OrElse (expr, otherExpr);
					} else {
						break;
					}
				}
				return expr;
			}

			Expression ParseConditionalTernaryExpression()
			{
				var expr = ParseLogicalOrExpression ();
				if (tokenizer.CurrentTokenType == TokenType.Operator &&
					tokenizer.CurrentTokenText == "?") {
					var i = tokenizer.CurrentTokenIndex;
					tokenizer.Read ();

					if (expr.Type != typeof(bool)) {
						throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
							string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(expr.Type)));
					}

					var trueExpr = ParseConditionalTernaryExpression ();
					if (tokenizer.CurrentTokenType != TokenType.Operator ||
						tokenizer.CurrentTokenText != ":") {
						throw MakeUnexpectedToken("operator :");
					}
					var falseExpr = ParseConditionalTernaryExpression ();
					if (trueExpr.Type != falseExpr.Type) {
						// Attempt to unification is not supported
						throw new InvalidQuerySemanticsException (tokenizer.Text, i,
							string.Format ("Cannot unify type '{0}' with '{1}'.",
								GetTypeName(trueExpr.Type), GetTypeName(falseExpr.Type)));
					}

					return Expression.Condition (expr, trueExpr, falseExpr);
				} else {
					return expr;
				}
			}

			ParsedQueryText ParseRoot()
			{
				var i = tokenizer.CurrentTokenIndex;
				var expr = ParseConditionalTernaryExpression ();
				if (tokenizer.CurrentTokenType != TokenType.End) {
					throw new InvalidQuerySyntaxException (tokenizer.Text, tokenizer.CurrentTokenIndex,
						string.Format("Extra token '{0}' found.", tokenizer.CurrentTokenDisplayName));
				}

				if (expr.Type != typeof(bool)) {
					throw new InvalidQuerySemanticsException (tokenizer.CurrentTokenText, i,
						string.Format("Cannot use '{0}' as a boolean value.", GetTypeName(expr.Type)));
				}

				result.Predicate = Expression.Lambda<Func<long, Ston.StonVariant, bool>> (expr,
					new [] {rowIdExpr, valueExpr});
				result.Parameters = new object[parameterNames.Count];
				result.ParameterNames = parameterNames.ToArray ();

				return result;
			}

			public static ParsedQueryText Parse(string text)
			{
				return new Parser (text).ParseRoot ();
			}
		}

	}
}


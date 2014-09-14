using System;

namespace Yavit.StellaDB
{
	public partial class PreparedTextQuery: PreparedQuery
	{
		readonly ParsedQueryText parsed;

		internal PreparedTextQuery (Table table, string text):
		base(table)
		{
			parsed = Parser.Parse (text);
		}

		int? FindParameter(string name)
		{
			var p = parsed.ParameterNames;
			for (int i = 0; i < p.Length; ++i)
				if (p [i] == name)
					return i;
			return null;
		}

		public object this [string parameterName]
		{
			get {
				var index = FindParameter (parameterName);
				if (index.HasValue) {
					return parsed.Parameters [index.Value];
				} else {
					return null;
				}
			}
			set {
				var index = FindParameter (parameterName);
				if (index.HasValue) {
					parsed.Parameters [index.Value] = value;
				}
			}
		}

		internal override System.Linq.Expressions.Expression<Func<long, Ston.StonVariant, bool>> Predicate {
			get {
				return parsed.Predicate;
			}
		}

	}
}


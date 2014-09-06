using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;

namespace Yavit.StellaDB.Indexer
{
	sealed class QueryOptimizer
	{
		public class IndexPart
		{
			public string Key;
			public KeyProvider KeyProvider;
		}
		public class Index
		{
			public readonly Indexer.Index BaseIndex;
			public readonly IndexPart[] Parts;
			public readonly double Cardinality;

			public Index(Indexer.Index baseIndex, double cardinality)
			{
				BaseIndex = baseIndex;
				Parts = (from i in baseIndex.GetFields()
					select new IndexPart() {
					Key = i.Name,
					KeyProvider = i.KeyProvider
					}).ToArray();
				Cardinality = cardinality;
			}
		}

		readonly List<Index> indices = new List<Index>();

		public class ProcessResult
		{
			// Either RowIdUsage or IndexUsage might become non-null.
			// When both are null, empty result set are returned
			public RowIdUsage RowIdUsage;
			public IndexUsage IndexUsage;

			// Processed expression.
			public Func<long, Ston.StonVariant, bool> Expression;

			public SortKey[] SortKeys;
		}

		public struct SortKey
		{
			public string Name; // null for Row ID
			public bool Descending;
		}

		public class RowIdUsage
		{
			// always inclusive.
			public long? StartRowId;
			public long? EndRowId;
			public bool Descending;
		}

		public class IndexUsage
		{
			public Index Index;
			public byte[] StartKey;
			public byte[] EndKey;
			public bool Descending;
			public bool StartInclusive;
			public bool EndInclusive;
		}

		public QueryOptimizer ()
		{
		}

		public void RegisterIndex(Index index)
		{
			if (index == null) {
				throw new ArgumentNullException ("index");
			}
			indices.Add (index);
		}

		void EnumerateClauses(Expression expr, Action<Expression> callback)
		{
			if (expr.NodeType == ExpressionType.AndAlso) {
				var bin = (BinaryExpression)expr;
				EnumerateClauses (bin.Left, callback);
				EnumerateClauses (bin.Right, callback);
			} else {
				callback (expr);
			}
		}

		string MapExpressionToIndexKey(Expression expr, Expression rootParamExpr)
		{
			if (!typeof(Ston.StonVariant).IsAssignableFrom(expr.Type)) {
				return null;
			}
			if (expr.NodeType == ExpressionType.Index) {
				var idxExpr = (IndexExpression)expr;
				if (idxExpr.Object != rootParamExpr) {
					return null;
				}

				var keyExpr = idxExpr.Arguments [0];
				if (keyExpr.NodeType != ExpressionType.Constant) {
					return null;
				}

				var constExpr = (ConstantExpression)keyExpr;
				if (constExpr.Type != typeof(string)) {
					return null;
				}

				var key = (string)constExpr.Value;
				return key;
			} else if (expr.NodeType == ExpressionType.Call) {
				var callExpr = (MethodCallExpression)expr;
				if (callExpr.Object != rootParamExpr) {
					return null;
				}
				if (callExpr.Method.Name != "get_Item" ||
					callExpr.Arguments.Count != 1) {
					return null;
				}

				var keyExpr = callExpr.Arguments[0];
				if (keyExpr.NodeType != ExpressionType.Constant) {
					return null;
				}

				var constExpr = (ConstantExpression)keyExpr;
				if (constExpr.Type != typeof(string)) {
					return null;
				}

				var key = (string)constExpr.Value;
				return key;
			}
			return null;
		}

		class CheckExpressionNotDependentOnParameterVisitor: ExpressionVisitor
		{
			public bool Result = true;

			protected override Expression VisitParameter (ParameterExpression node)
			{
				Result = false;
				return base.VisitParameter (node);
			}
		}

		bool CheckExpressionNotDependentOnParameter(Expression expr)
		{
			var visitor = new CheckExpressionNotDependentOnParameterVisitor ();
			visitor.Visit (expr);
			return visitor.Result;
		}

		enum ClauseType
		{
			Equal,
			LessThan,
			LessThanOrEqual,
			GreaterThan,
			GreaterThanOrEqual
		}
		class Clause
		{
			public string Name;
			public ClauseType Type;
			public Expression Right;
		}

		static ClauseType FlipClauseType(ClauseType t)
		{
			switch (t) {
			case ClauseType.Equal:
				return ClauseType.Equal;
			case ClauseType.LessThan:
				return ClauseType.GreaterThan;
			case ClauseType.LessThanOrEqual:
				return ClauseType.GreaterThanOrEqual;
			case ClauseType.GreaterThan:
				return ClauseType.LessThan;
			case ClauseType.GreaterThanOrEqual:
				return ClauseType.LessThanOrEqual;
			default:
				throw new ArgumentOutOfRangeException ();
			}
		}

		public Func<ProcessResult> Process(Expression<Func<long, Ston.StonVariant, bool>> expr,
			SortKey[] sortKeys)
		{
			var rowIdParamExpr = expr.Parameters [0];
			var rootParamExpr = expr.Parameters [1];
			var rowIdUsage = new RowIdUsage ();
			var clauses = new Dictionary<string, List<Clause>>();
			var rowIdClauses = new List<Clause> ();
			Clause cl = new Clause();

			EnumerateClauses (expr.Body, clause => {
				var type = clause.NodeType;
				switch (type) {
				case ExpressionType.LessThan:
					cl.Type = ClauseType.LessThan;
					break;
				case ExpressionType.LessThanOrEqual:
					cl.Type = ClauseType.LessThanOrEqual;
					break;
				case ExpressionType.GreaterThan:
					cl.Type = ClauseType.GreaterThan;
					break;
				case ExpressionType.GreaterThanOrEqual:
					cl.Type = ClauseType.GreaterThanOrEqual;
					break;
				case ExpressionType.Equal:
					cl.Type = ClauseType.Equal;
					break;

					// note: "not-equal" cannot be optimized with index
				default:
					return;
				}

				var binExpr = (BinaryExpression) clause;
				var keyLeft = MapExpressionToIndexKey(binExpr.Left, rootParamExpr);
				var keyRight = MapExpressionToIndexKey(binExpr.Right, rootParamExpr);

				// Only "variable-constant" can be optimized
				if (keyLeft == null && keyRight != null ||
					keyLeft != null && keyRight == null) {
					if (keyRight != null) {
						// swap left/right
						cl.Right = binExpr.Left;
						cl.Name = keyRight;
						cl.Type = FlipClauseType(cl.Type);
					} else {
						cl.Right = binExpr.Right;
						cl.Name = keyLeft;
					}

					if (CheckExpressionNotDependentOnParameter(cl.Right)) {
						List<Clause> list;
						if (!clauses.TryGetValue(cl.Name, out list)) {
							list = new List<Clause>();
							clauses.Add(cl.Name, list);
						}

						list.Add(cl);
						return;
					}

				}

				// "RowId-constant"
				if (binExpr.Left == rowIdParamExpr && binExpr.Right != rowIdParamExpr ||
					binExpr.Left != rowIdParamExpr && binExpr.Right == rowIdParamExpr) {
					cl.Name = null;
					if (binExpr.Right == rowIdParamExpr) {
						// swap left/right
						cl.Right = binExpr.Left;
						cl.Type = FlipClauseType(cl.Type);
					} else {
						cl.Right = binExpr.Right;
					}

					if (CheckExpressionNotDependentOnParameter(cl.Right) &&
						cl.Right.Type == typeof(long)) {
						rowIdClauses.Add(cl);
					}
				}

				// clause -- done
			});

			// When row ID is used, use it for optimization.
			if (rowIdClauses.Count > 0) {
				return ProcessWithRowIdIndex (expr, rowIdClauses, sortKeys);
			}

			// Optimize with keys
			var bestIndices = from index in indices
			                  where index.Parts.All (part => clauses.ContainsKey (part.Key))
							  orderby index.Cardinality descending
			                  select index;
			var bestIndex = bestIndices.FirstOrDefault ();
			if (bestIndex != null) {
				try {
					return ProcessWithIndex (expr, clauses, bestIndex, sortKeys);
				} catch (Ston.StonVariantException) { }
			}

			// Optimization failure.
			return ProcessWithRowIdIndex (expr, rowIdClauses, sortKeys);
		}

		sealed class FieldRange
		{
			public Ston.StonVariant MinValue;
			public Ston.StonVariant MaxValue;
			public bool IsMinExclusive;
			public bool IsMaxExclusive;
		}

		Func<ProcessResult> ProcessWithIndex(Expression<Func<long, Ston.StonVariant, bool>> expr,
			Dictionary<string, List<Clause>> clauses, Index index, SortKey[] sortKeys)
		{
			var compiledClausesEnumerable =
				index.Parts.Select (part => {
					List<Clause> list;
					clauses.TryGetValue(part.Key, out list);
					if (list == null) {
						return null;
					}

					return from clause in list
						select new {
						Type = clause.Type,
						Func = Expression.Lambda<Func<object>>(Expression.Convert(clause.Right, typeof(object))).Compile()
					};
				});
			var compiledClauses = compiledClausesEnumerable.ToArray ();
			var compiledExpr = expr.Compile ();
			var ranges = new FieldRange[compiledClauses.Length];
			byte[] startKey = new byte[index.BaseIndex.KeyLength];
			byte[] endKey = new byte[index.BaseIndex.KeyLength];

			return () => {
				for (int i = 0; i < ranges.Length; ++i) {
					Ston.StonVariant minValue = null;
					Ston.StonVariant maxValue = null;
					bool minExclusive = false, maxExclusive = false;

					var clList = compiledClauses[i];
					if (clList != null) {
						foreach (var clause in clList) {
							try {
								var val = clause.Func();
								if (val == null) {
									continue;
								}
								switch (clause.Type) {
								case ClauseType.GreaterThan:
									if (object.ReferenceEquals(minValue, null) || minValue.CompareTo(val) < 0) {
										minValue = new Ston.StaticStonVariant(val);
										minExclusive = true;
									} else if (minValue.CompareTo(val) == 0) {
										minExclusive = true;
									}
									break;
								case ClauseType.Equal:
								case ClauseType.GreaterThanOrEqual:
									if (object.ReferenceEquals(minValue, null) || minValue.CompareTo(val) < 0) {
										minValue = new Ston.StaticStonVariant(val);
										minExclusive = false;
									}
									break;
								}
								switch (clause.Type) {
								case ClauseType.LessThan:
									if (object.ReferenceEquals(maxValue, null) || maxValue.CompareTo(val) > 0) {
										maxValue = new Ston.StaticStonVariant(val);
										maxExclusive = true;
									} else if (maxValue.CompareTo(val) == 0) {
										maxExclusive = true;
									}
									break;
								case ClauseType.Equal:
								case ClauseType.LessThanOrEqual:
									if (object.ReferenceEquals(maxValue, null) || maxValue.CompareTo(val) > 0) {
										maxValue = new Ston.StaticStonVariant(val);
										maxExclusive = false;
									}
									break;
								}
							} catch (Ston.StonVariantException) {}
						}
					}

					if (ranges[i] == null) {
						ranges[i] = new FieldRange();
					}
					var fr = ranges[i];
					fr.MinValue = minValue;
					fr.MaxValue = maxValue;
					fr.IsMinExclusive = minExclusive;
					fr.IsMaxExclusive = maxExclusive;
				}

				var result = new ProcessResult();
				result.IndexUsage = new IndexUsage();
				result.Expression = compiledExpr;

				var minValues = new object[ranges.Length];
				var maxValues = new object[ranges.Length];

				for (int i = 0; i < ranges.Length; ++i) {
					minValues[i] = Indexer.Index.InfimumFieldValue;
					maxValues[i] = Indexer.Index.SupremumFieldValue;
				}

				bool lastMinExclusive = false;
				bool lastMaxExclusive = false;
				bool hasRanged = false;
				for (int i = 0; i < ranges.Length; ++i) {
					var fr = ranges[i];
					if (!hasRanged && fr != null && !object.ReferenceEquals(fr.MinValue, null) &&
						fr.MinValue.Value != null) {
						minValues[i] = fr.MinValue.Value;
						lastMinExclusive = fr.IsMinExclusive;
					} else {
						minValues[i] = lastMinExclusive ? 
							Indexer.Index.SupremumFieldValue : Indexer.Index.InfimumFieldValue;
					}
					if (!hasRanged && fr != null && !object.ReferenceEquals(fr.MaxValue, null) &&
						fr.MaxValue.Value != null) {
						maxValues[i] = fr.MaxValue.Value;
						lastMaxExclusive = fr.IsMaxExclusive;
					} else {
						maxValues[i] = lastMaxExclusive ? 
							Indexer.Index.InfimumFieldValue : Indexer.Index.SupremumFieldValue;
					}
					if (maxValues[i] == Indexer.Index.InfimumFieldValue || 
						minValues[i] == Indexer.Index.InfimumFieldValue ||
						maxValues[i] == Indexer.Index.SupremumFieldValue || 
						minValues[i] == Indexer.Index.SupremumFieldValue ||
						fr.MinValue.CompareTo(fr.MaxValue.Value) != 0) {
						hasRanged = true;
					} else if (fr.IsMaxExclusive || fr.IsMinExclusive) {
						return null;
					}

				}

				index.BaseIndex.EncodeKeyByFieldValues(lastMinExclusive ? long.MaxValue : 0, minValues,
					startKey, 0);
				index.BaseIndex.EncodeKeyByFieldValues(lastMaxExclusive ? 0 : long.MaxValue, maxValues,
					endKey, 0);

				result.IndexUsage.StartKey = startKey;
				result.IndexUsage.EndKey = endKey;
				result.IndexUsage.StartInclusive = !lastMinExclusive;
				result.IndexUsage.EndInclusive = !lastMaxExclusive;
				result.IndexUsage.Descending = false;
				result.IndexUsage.Index = index;

				// TODO: take sorting into account

				result.SortKeys = sortKeys;

				return result;
			};
		}

		Func<ProcessResult> ProcessWithRowIdIndex(Expression<Func<long, Ston.StonVariant, bool>> expr,
			List<Clause> clauses, SortKey[] sortKeys)
		{
			var compiledClausesEnumerable =
				from clause in clauses
				where clause.Name == null
				select new {
					Type = clause.Type,
					Func = Expression.Lambda<Func<long>>(clause.Right).Compile()
				};
			var compiledClauses = compiledClausesEnumerable.ToArray ();
			var compiledExpr = expr.Compile ();

			return () => {
				long? minRowId = null;
				long? maxRowId = null;
				foreach (var clause in compiledClauses)
				{
					var val = clause.Func();
					var val2 = val;
					switch (clause.Type) {
					case ClauseType.GreaterThan:
						if (val2 == long.MaxValue) {
							return null; // empty result
						}
						++val2;
						goto case ClauseType.Equal;
					case ClauseType.Equal:
					case ClauseType.GreaterThanOrEqual:
						if (minRowId == null || val2 < (long)minRowId) {
							minRowId = val2;
						}
						break;
					}
					val2 = val;
					switch (clause.Type) {
					case ClauseType.LessThan:
						if (val2 <= 0) {
							return null; // empty result
						}
						--val2;
						goto case ClauseType.Equal;
					case ClauseType.Equal:
					case ClauseType.LessThanOrEqual:
						if (maxRowId == null || val2 > (long)maxRowId) {
							maxRowId = val2;
						}
						break;
					}
				}

				var result = new ProcessResult();
				result.RowIdUsage = new RowIdUsage();
				result.Expression = compiledExpr;

				int firstNonOptimizedSortKeyIndex = 0;
				while (firstNonOptimizedSortKeyIndex < sortKeys.Length) {
					var sortKey = sortKeys[firstNonOptimizedSortKeyIndex];
					if (sortKey.Name == null) {
						result.RowIdUsage.Descending = true;
					} else {
						break;
					}
					++firstNonOptimizedSortKeyIndex;
				}
				/*
				if (firstNonOptimizedSortKeyIndex > 0) {
					result.SortKeys = sortKeys.Skip(firstNonOptimizedSortKeyIndex).ToArray();
				} else {*/
					result.SortKeys = sortKeys;
				//}

				if (result.RowIdUsage.Descending) {
					result.RowIdUsage.StartRowId = maxRowId;
					result.RowIdUsage.EndRowId = minRowId;
				} else {
					result.RowIdUsage.StartRowId = minRowId;
					result.RowIdUsage.EndRowId = maxRowId;
				}

				return result;
			};
		}
	
	}
}


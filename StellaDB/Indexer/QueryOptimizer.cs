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
			public IndexPart[] Parts;
			public double Cardinality;
		}

		readonly List<Index> indices = new List<Index>();

		public class ProcessResult
		{
			// Either RowIdUsage or IndexUsage becomes non-null
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
		struct Clause
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
					if (binExpr.Right == rowIdParamExpr) {
						// swap left/right
						cl.Right = binExpr.Left;
						cl.Type = FlipClauseType(cl.Type);
					} else {
						cl.Right = binExpr.Right;
					}

					if (CheckExpressionNotDependentOnParameter(cl.Right) ||
						cl.Right.Type != typeof(long)) {
						rowIdClauses.Add(cl);
					}
				}

				// clause -- done
			});

			// When row ID is used, use it for optimization.
			if (rowIdClauses.Count > 0) {
				return ProcessWithRowIdIndex (expr, rowIdClauses, sortKeys);
			}

			// TODO: optimize with keys


			// Optimization failure.
			return ProcessWithRowIdIndex (expr, rowIdClauses, sortKeys);
		}

		Func<ProcessResult> ProcessWithRowIdIndex(Expression<Func<long, Ston.StonVariant, bool>> expr,
			List<Clause> clauses, SortKey[] sortKeys)
		{
			var compiledClauses =
				from clause in clauses
				select new {
					Type = clause.Type,
					Func = Expression.Lambda<Func<long>>(clause.Right).Compile()
				};

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
				result.Expression = expr.Compile();

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


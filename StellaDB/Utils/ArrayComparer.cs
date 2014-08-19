using System;
using System.Collections.Generic;

namespace Yavit.StellaDB.Utils
{
	sealed class ArrayComparer<T>: IEqualityComparer<T[]>
	{
		public static readonly ArrayComparer<T> Default = new ArrayComparer<T>();

		readonly IEqualityComparer<T> comparer;
		public ArrayComparer ()
		{
			comparer = EqualityComparer<T>.Default;
		}
		public ArrayComparer (IEqualityComparer<T> comparer)
		{
			if (comparer == null)
				throw new ArgumentNullException ("comparer");
			this.comparer = comparer;
		}

		#region IEqualityComparer implementation

		public bool Equals (T[] x, T[] y)
		{
			if (x == null && y == null) {
				return true;
			} else if (x == null || y == null ||
				x.Length != y.Length) {
				return false;
			}

			var eq = comparer;
			for (int i = 0; i < x.Length; ++i) {
				if (!eq.Equals(x[i], y[i])) {
					return false;
				}
			}
			return true;
		}

		public int GetHashCode (T[] obj)
		{
			if (obj == null) {
				return 114514;
			}
			int hash = 3;
			var eq = comparer;
			foreach (var e in obj)
			{
				hash = (hash * 21) + eq.GetHashCode (e);
			}
			return hash;
		}

		#endregion

	}
}


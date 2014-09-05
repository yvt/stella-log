using System;

namespace Yavit.StellaLog.Core.Utils
{
	static class ArrayUtils
	{
		public static TSource Last<TSource>(this TSource[] array)
		{
			if (array.Length == 0) {
				throw new InvalidOperationException ();
			}
			return array [array.Length - 1];
		}
	}
}


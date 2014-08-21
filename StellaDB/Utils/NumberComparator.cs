using System;

namespace Yavit.StellaDB.Utils
{
	static class NumberComparatorExtension
	{
		// Numeric comparsion by promoting long to double gives
		// the wrong result in some cases.
		// This method does it in the correct way.
		public static int CompareTo2(this double x, long y)
		{
			if (x >= 9223372036854775808.0) { // larger than long.MaxValue
				return 1;
			} else if (x < -9223372036854775808.0) { // smaller than long.MinValue 
				return -1;
			}
			return ((long)x).CompareTo (y);
		}
		public static int CompareTo2(this double x, ulong y)
		{
			if (x > 18446744073709549568.0) { // larger than ulong.MaxValue
				return 1;
			} else if (x < 0.0) { // smaller than ulong.MinValue 
				return -1;
			}
			return ((long)x).CompareTo (y);
		}
		public static int CompareTo2(this long x, ulong y)
		{
			if (x < 0) {
				return -1;
			} else {
				return ((ulong)x).CompareTo (y);
			}
		}
		public static int CompareTo2(this long x, double y)
		{
			return -CompareTo2(y, x);
		}
		public static int CompareTo2(this ulong x, double y)
		{
			return -CompareTo2(y, x);
		}
		public static int CompareTo2(this ulong x, long y)
		{
			return -CompareTo2(y, x);
		}

	}
}


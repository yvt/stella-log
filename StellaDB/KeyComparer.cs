using System;

namespace Yavit.StellaDB
{
	public interface IKeyComparer: System.Collections.Generic.IEqualityComparer<byte[]>
	{
		int Compare(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2);

		bool IsValidKey(byte[] key);
	}

	public sealed class DefaultKeyComparer: IKeyComparer, System.Collections.Generic.IEqualityComparer<byte[]>
	{
		public static readonly DefaultKeyComparer Instance = new DefaultKeyComparer();

		public int Compare (byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2)
		{
			int i = 0;
			int len = Math.Min (length1, length2);
			for (; i < len; ++i) {
				var a = buffer1 [offset1];
				var b = buffer2 [offset2];
				++offset1; ++offset2;
				if (a > b)
					return 1;
				else if (a < b)
					return -1;
			}
			if (i == length1) {
				if (i == length2) {
					return 0;
				} else {
					return -1;
				}
			} else {
				return 1;
			}
		}

		public bool Equals (byte[] x, byte[] y)
		{
			if (x.Length != y.Length) {
				return false;
			}
			for (int i = 0; i < x.Length; ++i) {
				if (x [i] != y [i])
					return false;
			}
			return true;
		}

		public int GetHashCode (byte[] obj)
		{
			int result = 17;
			foreach (var i in obj) {
				result = i + result * 23;
			}
			return result;
		}

		public bool IsValidKey(byte[] key) { return true; }

	}

	public sealed class ReversedKeyComparer: IKeyComparer, System.Collections.Generic.IEqualityComparer<byte[]>
	{
		public static readonly ReversedKeyComparer Instance = new ReversedKeyComparer();

		public int Compare (byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2)
		{
			int i = 0;
			int len = Math.Min (length1, length2);
			offset1 += length1; offset2 += length2;
			for (; i < len; ++i) {
				--offset1; --offset2;
				var a = buffer1 [offset1];
				var b = buffer2 [offset2];
				if (a > b)
					return 1;
				else if (a < b)
					return -1;
			}
			if (i == length1) {
				if (i == length2) {
					return 0;
				} else {
					return -1;
				}
			} else {
				return 1;
			}
		}

		public bool Equals (byte[] x, byte[] y)
		{
			if (x.Length != y.Length) {
				return false;
			}
			for (int i = 0; i < x.Length; ++i) {
				if (x [i] != y [i])
					return false;
			}
			return true;
		}

		public int GetHashCode (byte[] obj)
		{
			int result = 17;
			foreach (var i in obj) {
				result = i + result * 23;
			}
			return result;
		}

		public bool IsValidKey(byte[] key) { return true; }

	}
}


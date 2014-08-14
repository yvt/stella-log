using System;

namespace Yavit.StellaDB.LowLevel
{
	/// <summary>
	/// Fixed size bit array.
	/// </summary>
	internal sealed class Bitmap
	{
		private byte[] bits;
		private int numOne;

		public Bitmap (byte[] bits)
		{
			if ((ulong)bits.Length >= ((ulong)int.MaxValue + 1) / 8)
			{
				throw new InvalidOperationException ("Bitmap too big.");
			}
			this.bits = bits;

			UpdateStatistics ();
		}

		public Bitmap (int numBits)
		{
			if (numBits < 0) {
				throw new ArgumentOutOfRangeException ("numBits");
			}
			bits = new byte[(numBits + 7) / 8];
			numOne = 0;
		}

		public bool this [int index] {
			get {
				return (bits [index >> 3] & (1UL << (index & 7))) != 0;
			}
			set {
				int aindex = index >> 3;
				int bit = index & 7;
				if (value) {
					if ((bits[aindex] & (1 << bit)) == 0) {
						++numOne;
					}
					bits [aindex] = (byte)(bits[aindex] | (1 << bit));
				} else {
					if ((bits[aindex] & (1 << bit)) != 0) {
						--numOne;
					}
					bits [aindex] = (byte)(bits[aindex] & ~(1 << bit));
				}
			}
		}

		public byte[] GetBuffer()
		{
			return bits;
		}

		public void SetBuffer(byte[] b)
		{
			bits = b;
			UpdateStatistics ();
		}

		public void UpdateStatistics() {
			numOne = 0;
			foreach (ulong bmp in bits) {
				numOne += InternalUtils.CountBitsSet (bmp);
			}
		}

		public int Size
		{
			get {
				return bits.Length * 8;
			}
		}

		public int NumOnes
		{
			get {
				return numOne;
			}
		}

		public int NumZeros
		{
			get {
				return Size - numOne;
			}
		}

		public int? FindOne()
		{
			return FindFirstOne ();
		}
		public int? FindFirstOne()
		{
			for (int i = 0; i < bits.Length; ++i) {
				if (bits[i] != 0) {
					var v = bits [i];
					int j = 0;
					while ((v & 1) == 0) {
						++j; v >>= 1;
					}
					return j + i * 8;
				}
			}
			return null;
		}

		public void FillOne()
		{
			for (int i = 0; i < bits.Length; ++i) {
				bits [i] = 0xff;
			}
			numOne = Size;
		}

		public void SetRanged(bool state, int start, int length)
		{
			// TODO: optimize

			if (start < 0 || start + length > Size) {
				throw new ArgumentOutOfRangeException ();
			}

			for (int i = 0; i < length; ++i) {
				this [start + i] = state;
			}
		}
	}
}


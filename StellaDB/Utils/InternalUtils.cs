using System;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB
{
	static class InternalUtils
	{
		private static uint[] crcTable;
		private static System.Security.Cryptography.RNGCryptoServiceProvider rng =
			new System.Security.Cryptography.RNGCryptoServiceProvider();

		private static byte[] bitCount8;

		// Get AggressiveInlining while targeting .NET 4.0
		public const MethodImplOptions MethodImplAggresiveInlining = (MethodImplOptions)256;

		static InternalUtils()
		{
			crcTable = new uint[256];
			for (uint i = 0; i < 256; ++i) {
				uint c = i;
				for (int j = 0; j < 8; ++j) {
					c = (c & 1) != 0 ? (0xedb88320 ^ (c >> 1)) : (c >> 1);
					crcTable [i] = c;
				}
			}

			bitCount8 = new byte[256];
			bitCount8 [0] = 0;
			for (int i = 1; i < 256; ++i) {
				bitCount8 [i] = (byte)((i & 1) + bitCount8 [i >> 1]);
			}
		}
		public static uint ComputeCrc32(byte[] bytes, int offset, int length) 
		{
			if (length < 0 || offset < 0 || 
				checked(offset + length) > bytes.Length) {
				throw new IndexOutOfRangeException ();
			}
			
			uint c = 0xffffffffU;
			for (; length != 0; --length, ++offset) {
				c = crcTable[(c ^ bytes[offset]) & 0xff] ^ (c >> 8);
			}
			return c ^ 0xffffffff;
		}

		public static void ZeroFill(byte[] bytes, int offset, int length)
		{
			if (bytes == null)
				throw new ArgumentNullException ("bytes");
			if (offset < 0)
				throw new ArgumentOutOfRangeException ("offset");
			if (offset + length > bytes.Length)
				throw new ArgumentOutOfRangeException ("length");
			for (; length > 0; --length)
			{
				bytes [offset++] = 0;
			}
		}

		public static int GetBitWidth(uint i) {
			int count = 0;
			while (i > 0) {
				++count; i >>= 1;
			}
			return count;
		}

		[MethodImpl(MethodImplAggresiveInlining)]
		public static int GetBitWidth(int i) {
			return GetBitWidth (checked((uint)i));
		}

		public static int GetBitWidth(ulong i) {
			int count = 0;
			while (i > 0) {
				++count; i >>= 1;
			}
			return count;
		}

		[MethodImpl(MethodImplAggresiveInlining)]
		public static int GetBitWidth(long i) {
			return GetBitWidth (checked((ulong)i));
		}

		[MethodImpl(MethodImplAggresiveInlining)]
		public static bool IsPowerOfTwo(uint i) {
			return (i & (i - 1)) == 0;
		}

		[MethodImpl(MethodImplAggresiveInlining)]
		public static bool IsPowerOfTwo(int i) {
			return IsPowerOfTwo (checked((uint)i));
		}

		#if false
		public static int CountBitsSet(byte b) {
			return bitCount8 [b];
		}
		public static int CountBitsSet(ushort b) {
			return bitCount8 [(b >> 8) & 0xff] + bitCount8 [b & 0xff];
		}
		public static int CountBitsSet(uint b) {
			int r = bitCount8 [b & 0xff]; b >>= 8;
			r += bitCount8 [b & 0xff]; b >>= 8;
			r += bitCount8 [b & 0xff]; b >>= 8;
			r += bitCount8 [b & 0xff];
			return r;
		}
		#else
		public static int CountBitsSet(uint b) {
			int r = 0;
			while (b != 0) {
				++r; b &= b - 1;
			}
			return r;
		}
		#endif
		[MethodImpl(MethodImplAggresiveInlining)]
		public static int CountBitsSet(ulong b) {
			return CountBitsSet ((uint)b) + CountBitsSet((uint)(b >> 32));
		}

		[MethodImpl(MethodImplAggresiveInlining)]
		public static int CountBitsSet(sbyte b) {
			return CountBitsSet((byte)b);
		}
		[MethodImpl(MethodImplAggresiveInlining)]
		public static int CountBitsSet(short b) {
			return CountBitsSet ((ushort)b);
		}
		[MethodImpl(MethodImplAggresiveInlining)]
		public static int CountBitsSet(int b) {
			return CountBitsSet ((uint)b);
		}
		[MethodImpl(MethodImplAggresiveInlining)]
		public static int CountBitsSet(long b) {
			return CountBitsSet ((ulong)b);
		}

		public static uint GenerateCryptographicRandomNumber() {
			var b = new byte[4];
			rng.GetBytes (b);
			return (uint)b [0] | ((uint)b [1] << 8) | ((uint)b [2] << 16) | ((uint)b [3] << 24);
		}

		public struct BitConverter
		{
			private readonly byte[] buffer;

			public byte[] Buffer
			{
				[MethodImpl(MethodImplAggresiveInlining)]
				get { return buffer; }
			}

			[MethodImpl(MethodImplAggresiveInlining)]
			public BitConverter(byte[] buffer)
			{
				this.buffer = buffer;
				if (buffer == null) {
					throw new ArgumentNullException("buffer");
				}
			}

			[MethodImpl(MethodImplAggresiveInlining)]
			public byte GetUInt8(int offset)
			{
				return buffer [offset];
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public ushort GetUInt16(int offset)
			{
				return (ushort)((uint)buffer[offset] | ((uint)buffer[offset + 1] << 8));
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public uint GetUInt32(int offset)
			{
				return (uint)buffer[offset] | ((uint)buffer[offset + 1] << 8)
					| ((uint)buffer[offset + 2] << 16) | ((uint)buffer[offset + 3] << 24);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public ulong GetUInt64(int offset)
			{
				return (ulong)GetUInt32 (offset) | ((ulong)GetUInt32 (offset + 4) << 32);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public sbyte GetInt8(int offset)
			{
				return (sbyte)GetUInt8 (offset);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public short GetInt16(int offset)
			{
				return (short)GetUInt16 (offset);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public int GetInt32(int offset)
			{
				return (int)GetUInt32 (offset);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public long GetInt64(int offset)
			{
				return (long)GetUInt64 (offset);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public double GetDouble(int offset)
			{
				return System.BitConverter.Int64BitsToDouble(GetInt64(offset));
			}

			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, byte b)
			{
				buffer [offset] = b;
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, ushort b)
			{
				buffer [offset] = (byte)(b);
				buffer [offset + 1] = (byte)(b >> 8);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, uint b)
			{
				buffer [offset] = (byte)(b);
				buffer [offset + 1] = (byte)(b >> 8);
				buffer [offset + 2] = (byte)(b >> 16);
				buffer [offset + 3] = (byte)(b >> 24);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, ulong b)
			{
				Set (offset, (uint)(b));
				Set (offset + 4, (uint)(b >> 32));
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, sbyte b)
			{
				Set (offset, (byte)b);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, short b)
			{
				Set (offset, (ushort)b);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, int b)
			{
				Set (offset, (uint)b);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, long b)
			{
				Set (offset, (ulong)b);
			}
			[MethodImpl(MethodImplAggresiveInlining)]
			public void Set(int offset, double b)
			{
				Set (offset, System.BitConverter.DoubleToInt64Bits(b));
			}


			[MethodImpl(MethodImplAggresiveInlining)]
			public ulong GetVariant(int offset, int numBytes)
			{
				ulong ret = 0;
				for(int i = 0; i < numBytes; ++i)
				{
					ret |= (ulong)(buffer [offset++] << (i << 3));
				}
				return ret;
			}

			[MethodImpl(MethodImplAggresiveInlining)]
			public void SetVariant(int offset, int numBytes, ulong value)
			{
				for (int i = 0; i < numBytes; ++i)
				{
					buffer [offset++] = (byte)(value >> (i << 3));
				}
			}

		}
	}
}


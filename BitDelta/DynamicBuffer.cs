using System;

namespace Yavit.BitDelta
{
	class DynamicBuffer
	{
		public byte[] Buffer = null;
		public int Length = 0;

		public void EnsureCapacity(int cap)
		{
			if (Buffer == null || cap > Buffer.Length) {
				int newSize = Math.Max (cap, Buffer != null ? Buffer.Length : 8) * 2;

				Array.Resize<byte> (ref Buffer, newSize);
			}
		}

		public void Write(byte b)
		{
			EnsureCapacity (Length + 1);
			Buffer [Length++] = b;
		}

		public void Write(byte[] b, int offset, int length)
		{
			EnsureCapacity(Length + length);
			System.Buffer.BlockCopy (b, offset, Buffer, Length, length);
			Length += length;
		}

		public byte[] ToArray()
		{
			if (Length == 0) {
				return new byte[0];
			}
			var b = new byte[Length];
			System.Buffer.BlockCopy (Buffer, 0, b, 0, Length);
			return b;
		}
	}
}


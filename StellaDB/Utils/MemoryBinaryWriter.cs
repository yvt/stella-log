using System;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.Utils
{
	class MemoryBinaryReader
	{
		protected byte[] buffer;
		protected InternalUtils.BitConverter cvt;
		protected int length = 0;
		protected int position = 0;

		internal MemoryBinaryReader()
		{
		}

		public MemoryBinaryReader(byte[] buffer, int length)
		{
			SetBuffer (buffer);
			this.length = length;
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public byte[] GetBuffer()
		{
			return buffer;
		}

		public void SetBuffer(byte[] b)
		{
			buffer = b;
			cvt = new InternalUtils.BitConverter(b);
		}

		public virtual int Length
		{
			get { return length; }
			set { throw new NotSupportedException (); }
		}

		public int Position
		{
			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			get { return position; }
			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			set { position = value; }
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		void CheckRead(int size)
		{
			if (position + size > length) {
				throw new IndexOutOfRangeException ();
			}
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public byte ReadUInt8() 
		{
			CheckRead (1);
			var r = cvt.GetUInt8 (position); position += 1;
			return r;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public ushort ReadUInt16() 
		{
			CheckRead (2);
			var r = cvt.GetUInt16 (position); position += 2;
			return r;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public uint ReadUInt32() 
		{
			CheckRead (4);
			var r = cvt.GetUInt32 (position); position += 4;
			return r;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public ulong ReadUInt64() 
		{
			CheckRead (8);
			var r = cvt.GetUInt64 (position); position += 8;
			return r;
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public sbyte ReadInt8() { return (sbyte)ReadUInt8(); }
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public short ReadInt16() { return (short)ReadUInt16(); }
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public int ReadInt32() { return (int)ReadUInt32(); }
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public long ReadInt64() { return (long)ReadUInt64(); }

		public void Read(byte[] buf, int offset, int len) {
			if (buf == null)
				throw new ArgumentNullException ("buf");
			if (offset < 0)
				throw new ArgumentOutOfRangeException ("offset");
			if (offset + len > buf.Length)
				throw new ArgumentOutOfRangeException ("len");
			CheckRead (len);
			Buffer.BlockCopy (buffer, position, buf, offset, len);
			position += len;
		}
	}

	sealed class MemoryBinaryWriter: MemoryBinaryReader
	{

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void EnsureCapacity(int len)
		{
			if (buffer == null || len > buffer.Length) {
				int newlen = Math.Max(checked(buffer == null ? 64 : buffer.Length), len) * 2;
				byte[] newBuffer = new byte[newlen];
				if (buffer != null) {
					Buffer.BlockCopy (buffer, 0, newBuffer, 0, length);
				}
				SetBuffer (newBuffer);
			}
		}

		public byte[] ToArray()
		{
			byte[] ret = new byte[length];
			if (length > 0) {
				Buffer.BlockCopy (buffer, 0, ret, 0, length);
			}
			return ret;
		}

		public override int Length
		{
			set {
				EnsureCapacity (value);
				length = value;
				// FIXME: zero truncated region
			}
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(byte[] b)
		{
			if (b == null)
				throw new ArgumentNullException ("b");
			Write (b, 0, b.Length);
		}

		public void Write(byte[] buf, int offset, int len)
		{
			if (buf == null)
				throw new ArgumentNullException ("buf");
			if (offset < 0)
				throw new ArgumentOutOfRangeException ("offset");
			if (offset + len > buf.Length)
				throw new ArgumentOutOfRangeException ("len");

			EnsureCapacity (position + len);
			Buffer.BlockCopy (buf, offset, buffer, position, len);
			position += len; length = Math.Max (length, position);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(byte b)
		{
			EnsureCapacity (position + 1);
			buffer [position] = b;
			position += 1; length = Math.Max (length, position);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(sbyte b)
		{
			Write ((byte)b);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(ushort b)
		{
			EnsureCapacity (position + 2);
			cvt.Set (position, b);
			position += 2; length = Math.Max (length, position);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(short b)
		{
			Write ((ushort)b);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(uint b)
		{
			EnsureCapacity (position + 4);
			cvt.Set (position, b);
			position += 4; length = Math.Max (length, position);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(int b)
		{
			Write ((uint)b);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(ulong b)
		{
			EnsureCapacity (position + 8);
			cvt.Set (position, b);
			position += 8; length = Math.Max (length, position);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public void Write(long b)
		{
			Write ((ulong)b);
		}

	}
}


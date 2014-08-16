using System;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.Ston
{
	public struct StonInteger: IFormattable, IComparable,
	IComparable<StonInteger>, IEquatable<StonInteger>, IConvertible
	{
		readonly ulong value;
		bool signed;

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public StonInteger(int value)
		{
			this.value = (ulong)(long)value;
			signed = true;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public StonInteger(long value)
		{
			this.value = (ulong)value;
			signed = true;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public StonInteger(ulong value)
		{
			this.value = value;
			signed = false;
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static implicit operator StonInteger (int value)
		{
			return new StonInteger ((long)value);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static implicit operator StonInteger (long value)
		{
			return new StonInteger (value);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static implicit operator StonInteger (ulong value)
		{
			return new StonInteger (value);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static explicit operator long (StonInteger value)
		{
			return value.ToInt64 ();
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static explicit operator ulong (StonInteger value)
		{
			return value.ToUInt64 ();
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static explicit operator double (StonInteger value)
		{
			if (value.signed)
				return (double)(long)value.value;
			else
				return (double)value.value;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public static explicit operator float (StonInteger value)
		{
			if (value.signed)
				return (float)(long)value.value;
			else
				return (float)value.value;
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public long ToInt64Unchecked()
		{
			return (long)value;
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public ulong ToUInt64Unchecked()
		{
			return value;
		}

		public override string ToString ()
		{
			return ToObject ().ToString ();
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public long ToInt64()
		{
			if (signed) {
				return unchecked((long)value);
			} else {
				return checked((long)value);
			}
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public ulong ToUInt64()
		{
			if (signed) {
				long l = unchecked((long)value);
				return checked((ulong)l);
			} else {
				return value;
			}
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public object ToObject()
		{
			if (signed) {
				return (long)value;
			} else {
				return value;
			}
		}
		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public decimal ToDecimal()
		{
			return value;
		}

		public bool TryConvertToInt64(out long ret)
		{
			if (signed || value <= (ulong)long.MaxValue) {
				ret = (long)value;
				return true;
			} else {
				ret = 0;
				return false;
			}
		}

		#region IFormattable implementation

		public string ToString (string format, IFormatProvider formatProvider)
		{
			if (signed) {
				return ((long)value).ToString (format, formatProvider);
			} else {
				return value.ToString (format, formatProvider);
			}
		}

		#endregion

		#region IComparable implementation

		int IComparable.CompareTo (object obj)
		{
			if (signed) {
				return ((long)value).CompareTo (obj);
			} else {
				return value.CompareTo (obj);
			}
		}

		#endregion

		#region IComparable implementation

		int IComparable<StonInteger>.CompareTo (StonInteger other)
		{
			if (signed) {
				if (other.signed) {
					return ((long)value).CompareTo ((long)other.value);
				} else {
					return ((long)value).CompareTo (other.value);
				}
			} else {
				if (other.signed) {
					return value.CompareTo ((long)other.value);
				} else {
					return value.CompareTo (other.value);
				}
			}
		}

		#endregion

		#region IEquatable implementation

		bool IEquatable<StonInteger>.Equals (StonInteger other)
		{
			if (signed) {
				if (other.signed) {
					return value == other.value;
				} else {
					return value == other.value && other.value <= (ulong)long.MaxValue;
				}
			} else {
				if (other.signed) {
					return value == other.value && other.value <= (ulong)long.MaxValue;
				} else {
					return value == other.value;
				}
			}
		}

		#endregion

		#region IConvertible implementation

		TypeCode IConvertible.GetTypeCode ()
		{
			return signed ? TypeCode.Int64 : TypeCode.UInt64;
		}

		bool IConvertible.ToBoolean (IFormatProvider provider)
		{
			return value != 0;
		}

		byte IConvertible.ToByte (IFormatProvider provider)
		{
			return checked((byte)ToUInt64());
		}

		char IConvertible.ToChar (IFormatProvider provider)
		{
			return checked((char)ToUInt64());
		}

		DateTime IConvertible.ToDateTime (IFormatProvider provider)
		{
			throw new InvalidCastException ("This conversion is not supported.");
		}

		decimal IConvertible.ToDecimal (IFormatProvider provider)
		{
			return ToDecimal ();
		}

		double IConvertible.ToDouble (IFormatProvider provider)
		{
			return (double)this;
		}

		short IConvertible.ToInt16 (IFormatProvider provider)
		{
			return checked((short)ToInt64());
		}

		int IConvertible.ToInt32 (IFormatProvider provider)
		{
			return checked((int)ToInt64());
		}

		long IConvertible.ToInt64 (IFormatProvider provider)
		{
			return ToInt64 ();
		}

		sbyte IConvertible.ToSByte (IFormatProvider provider)
		{
			return checked((sbyte)ToInt64());
		}

		float IConvertible.ToSingle (IFormatProvider provider)
		{
			return (float)this;
		}

		string IConvertible.ToString (IFormatProvider provider)
		{
			return ((IConvertible)ToObject()).ToString(provider);
		}

		object IConvertible.ToType (Type conversionType, IFormatProvider provider)
		{
			return ((IConvertible)ToObject ()).ToType (conversionType, provider);
		}

		ushort IConvertible.ToUInt16 (IFormatProvider provider)
		{
			return checked((ushort)ToUInt64());
		}

		uint IConvertible.ToUInt32 (IFormatProvider provider)
		{
			return checked((uint)ToUInt64());
		}

		ulong IConvertible.ToUInt64 (IFormatProvider provider)
		{
			return ToUInt64 ();
		}

		#endregion
	}

}


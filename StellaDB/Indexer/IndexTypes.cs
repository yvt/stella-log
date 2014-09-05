using System;
using Yavit.StellaDB.Utils;

namespace Yavit.StellaDB.Indexer
{


	abstract class KeyProvider
	{
		public abstract IKeyComparer KeyComparer { get; }
		public abstract KeyParameter Parameters { get; }
		public abstract int KeyLength { get; }
		public abstract bool EncodeKey(object value, byte[] output, int offset);
		public abstract void EncodeSupremum (byte[] output, int offset);
		public abstract void EncodeInfimum (byte[] output, int offset);
		public abstract bool SupportsType(Type type);
	}

	public abstract class KeyParameter
	{
		internal abstract KeyProvider CreateKeyProvider();
	}

	public sealed class BinaryKeyParameters: KeyParameter
	{
		public int KeyLength { get; set; }

		internal override KeyProvider CreateKeyProvider ()
		{
			return new BinaryKeyProvider (KeyLength);
		}
	}
	sealed class BinaryKeyProvider: KeyProvider
	{
		readonly int keyLength;

		public BinaryKeyProvider(int keyLength)
		{
			if (keyLength < 1) {
				throw new ArgumentOutOfRangeException ("keyLength");
			}
			this.keyLength = keyLength;
		}

		public override bool EncodeKey (object value, byte[] output, int offset)
		{
			var b = value as byte[];
			if (b == null) {
				return false;
			}

			int filllen = Math.Min (keyLength, b.Length);
			Buffer.BlockCopy (b, 0, output, offset, filllen);
			InternalUtils.ZeroFill (output, offset + filllen, keyLength - filllen);
			return true;
		}

		public override void EncodeSupremum (byte[] output, int offset)
		{
			for (int i = offset, count = keyLength; count > 0; --count, ++i)
				output [i] = 255;
		}

		public override void EncodeInfimum (byte[] output, int offset)
		{
			for (int i = offset, count = keyLength; count > 0; --count, ++i)
				output [i] = 0;
		}

		public override bool SupportsType (Type type)
		{
			return typeof(byte[]).IsAssignableFrom (type);
		}

		public override IKeyComparer KeyComparer {
			get {
				return DefaultKeyComparer.Instance;
			}
		}

		public override KeyParameter Parameters {
			get {
				return new BinaryKeyParameters () {
					KeyLength = keyLength
				};
			}
		}

		public override int KeyLength {
			get {
				return keyLength;
			}
		}

	}

	public sealed class NumericKeyParameters: KeyParameter
	{
		internal override KeyProvider CreateKeyProvider ()
		{
			return NumericKeyProvider.Instance;
		}
	}

	sealed class NumericKeyProvider: KeyProvider
	{
		// Numeric key is represented with one of these three data types.
		// When enncoding the key, the topmost data type must be used to
		// ensure uniqueness of the representation.
		enum NumberType: byte
		{
			Int64 = 0,
			UInt64 = 1,
			Double = 2
		}
		public static readonly NumericKeyProvider Instance = new NumericKeyProvider();

		sealed class NumericKeyComparer: IKeyComparer
		{
			public static readonly NumericKeyComparer Instance = new NumericKeyComparer();

			public int Compare (byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2)
			{
				if (length1 != 9 || length2 != 9) {
					throw new InvalidOperationException ("Invalid key length.");
				}
				var bc1 = new InternalUtils.BitConverter (buffer1);
				var bc2 = new InternalUtils.BitConverter (buffer2);
				var type1 = (NumberType)buffer1 [offset1];
				var type2 = (NumberType)buffer2 [offset2];

				switch (type1) {
				case NumberType.Int64:
					switch (type2) {
					case NumberType.Int64:
						return bc1.GetInt64 (offset1).CompareTo
							(bc2.GetInt64(offset2));
					case NumberType.UInt64:
						return -1;
					case NumberType.Double:
						return bc1.GetInt64 (offset1).CompareTo
							(bc2.GetDouble(offset2));
					}
					break;
				case NumberType.UInt64:
					switch (type2) {
					case NumberType.Int64:
						return 1;
					case NumberType.UInt64:
						return bc1.GetUInt64 (offset1).CompareTo
							(bc2.GetUInt64 (offset2));
					case NumberType.Double:
						return bc1.GetUInt64 (offset1).CompareTo
							(bc2.GetDouble (offset2));
					}
					break;
				case NumberType.Double:
					switch (type2) {
					case NumberType.Int64:
						return bc1.GetDouble (offset1).CompareTo
							(bc2.GetInt64(offset2));
					case NumberType.UInt64:
						return bc1.GetDouble (offset1).CompareTo
							(bc2.GetUInt64(offset2));
					case NumberType.Double:
						return bc1.GetDouble (offset1).CompareTo
							(bc2.GetDouble(offset2));
					}
					break;
				}
				throw new InvalidOperationException ("Invalid key.");
			}

			public bool IsValidKey (byte[] key)
			{
				return Enum.IsDefined(typeof(NumberType), key[0]) &&
					key.Length == 9;
			}

			public bool Equals (byte[] x, byte[] y)
			{
				return Compare (x, 0, x.Length, y, 0, y.Length) == 0;
			}

			public int GetHashCode (byte[] buffer)
			{
				if (buffer.Length != 9) {
					throw new InvalidOperationException ("Invalid key length.");
				}
				return DefaultKeyComparer.Instance.GetHashCode (buffer);
			}

		}

		void EncodeInt64(long v, byte[] output, int offset)
		{
			var bc = new InternalUtils.BitConverter (output);
			output [offset] = (byte)NumberType.Int64;
			bc.Set (offset + 1, v);
		}
		void EncodeUInt64(ulong v, byte[] output, int offset)
		{
			// Try encoding value with long
			if (v <= (ulong)long.MaxValue) {
				EncodeInt64 ((long)v, output, offset);
				return;
			}
			var bc = new InternalUtils.BitConverter (output);
			output [offset] = (byte)NumberType.UInt64;
			bc.Set (offset + 1, v);
		}
		bool EncodeDouble(double v, byte[] output, int offset)
		{
			// Try encoding value with ulong/long
			if (Math.Floor(v) == v && v >= (double)long.MinValue) {
				if (v <= (double)long.MaxValue) {
					EncodeInt64 ((long)v, output, offset);
					return true;
				} else if (v <= (double)ulong.MaxValue) {
					EncodeUInt64 ((ulong)v, output, offset);
					return true;
				}
			}

			// NaN cannot be ordered
			if (double.IsNaN(v)) {
				return false;
			}

			var bc = new InternalUtils.BitConverter (output);
			output [offset] = (byte)NumberType.Double;
			bc.Set (offset + 1, v);
			return true;
		}

		public override int KeyLength {
			get {
				return 9;
			}
		}

		public override bool EncodeKey (object value, byte[] output, int offset)
		{
			if (value == null) {
				return false;
			}

			var convertible = value as IConvertible;
			if (convertible == null) {
				return false;
			}

			switch (convertible.GetTypeCode ()) {
			case TypeCode.SByte:
			case TypeCode.Byte:
			case TypeCode.Int16:
			case TypeCode.UInt16:
			case TypeCode.Int32:
			case TypeCode.UInt32:
			case TypeCode.Int64:
				EncodeInt64 (convertible.ToInt64 (null), output, offset);
				return true;
			case TypeCode.UInt64:
				EncodeUInt64 (convertible.ToUInt64 (null), output, offset);
				return true;
			case TypeCode.Single:
			case TypeCode.Double:
				return EncodeDouble (convertible.ToDouble (null), output, offset);
			case TypeCode.Decimal: // unsupported
				return false;
			default:
				return false;
			}
		}

		public override void EncodeSupremum (byte[] output, int offset)
		{
			EncodeDouble (double.PositiveInfinity, output, offset);
		}

		public override void EncodeInfimum (byte[] output, int offset)
		{
			EncodeDouble (double.NegativeInfinity, output, offset);
		}

		static readonly System.Collections.Generic.HashSet<Type> types =
			new System.Collections.Generic.HashSet<Type>() {
			typeof(sbyte), typeof(short), typeof(int), typeof(long),
			typeof(byte), typeof(ushort), typeof(uint), typeof(ulong),
			typeof(float), typeof(double)
		};

		public override bool SupportsType(Type type) {
			return types.Contains (type);
		}

		public override IKeyComparer KeyComparer {
			get {
				return NumericKeyComparer.Instance;
			}
		}

		public override KeyParameter Parameters {
			get {
				return new NumericKeyParameters();
			}
		}
	}

}


using System;
using System.Collections.Generic;
using Yavit.StellaDB.Utils;

namespace Yavit.StellaDB.Ston
{
	/// <summary>
	/// Class that represents a value of STON. Used for easier query condition construction, and
	/// direct access of the member of serialized STON.
	/// </summary>
	public abstract class StonVariant: IComparable<double>, IComparable<long>, IComparable<ulong>
	{

		/// <summary>
		/// Polymorphic value of the instance;
		/// </summary>
		/// <value>The value.</value>
		public abstract object Value { get; }

		/// <summary>
		/// Gets the child <see cref="Yavit.StellaDB.Ston.StonVariant"/> at the specified index.
		/// If the <see cref="Yavit.StellaDB.Ston.StonVariant"/> is not a list, or index
		/// is out of the bounds, then this throws 
		/// <see cref="Yavit.StellaDB.Ston.StonVariantException"/>. 
		/// </summary>
		/// <exception cref="Yavit.StellaDB.Ston.StonVariantException">This instance 
		/// is not a list or the index is out of the bounds.</exception>
		/// <param name="index">Index.</param>
		public virtual StonVariant this [int index] { 
			get {
				var lst = Value as IList<object>;
				if (lst == null) {
					throw new StonVariantException ();
				}
				if (index < 0 || index >= lst.Count) {
					throw new StonVariantException ();
				}
				return new StaticStonVariant(lst [index]);
			}
		}

		/// <summary>
		/// Gets the child <see cref="Yavit.StellaDB.Ston.StonVariant"/> with the specified key.
		/// If the <see cref="Yavit.StellaDB.Ston.StonVariant"/> is not a dictionary, or the key
		/// was not found, then this throws 
		/// <see cref="Yavit.StellaDB.Ston.StonVariantException"/>. 
		/// </summary>
		/// <exception cref="Yavit.StellaDB.Ston.StonVariantException">This instance 
		/// is not a dictionary or the key was not found.</exception>
		/// <param name="key">The key.</param>
		public virtual StonVariant this [string key] { 
			get {
				var dic = Value as IDictionary<string, object>;
				if (dic == null) {
					throw new StonVariantException ();
				}
				object ret;
				if (dic.TryGetValue(key, out ret)) {
					return new StaticStonVariant (ret);
				} else {
					throw new StonVariantException ();
				}
			}
		}

		public override bool Equals (object obj)
		{
			var convertible1 = Value as IConvertible;
			if (convertible1 == null)
				return object.Equals (this, obj);
			var convertible2 = obj as IConvertible;
			if (convertible2 == null)
				return object.Equals (this, obj);
			switch (convertible1.GetTypeCode ()) {
			case TypeCode.String:
				if (convertible2.GetTypeCode() == TypeCode.String) {
					return string.Equals (convertible1.ToString (null), convertible2.ToString (null));
				}
				break;
			case TypeCode.SByte:
			case TypeCode.Byte:
			case TypeCode.Int16:
			case TypeCode.UInt16:
			case TypeCode.Int32:
			case TypeCode.UInt32:
			case TypeCode.Int64:
				var lval = convertible1.ToInt64 (null);
				switch (convertible2.GetTypeCode ()) {
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
					return lval == convertible2.ToInt64 (null);
				case TypeCode.UInt64:
					return lval.CompareTo2 (convertible2.ToUInt64 (null)) == 0;
				case TypeCode.Single:
				case TypeCode.Double:
					return lval.CompareTo2 (convertible2.ToDouble (null)) == 0;
				default:
					break;
				}
				break;
			case TypeCode.UInt64:
				var ulval = convertible1.ToUInt64 (null);
				switch (convertible2.GetTypeCode ()) {
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
					return ulval.CompareTo2 (convertible2.ToInt64 (null)) == 0;
				case TypeCode.UInt64:
					return ulval == convertible2.ToUInt64 (null);
				case TypeCode.Single:
				case TypeCode.Double:
					return ulval.CompareTo2 (convertible2.ToDouble (null)) == 0;
				default:
					break;
				}
				break;
			case TypeCode.Single:
			case TypeCode.Double:
				var dval = convertible1.ToDouble (null);
				switch (convertible2.GetTypeCode ()) {
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
					return dval.CompareTo2 (convertible2.ToInt64 (null)) == 0;
				case TypeCode.UInt64:
					return dval.CompareTo2 (convertible2.ToUInt64 (null)) == 0;
				case TypeCode.Single:
				case TypeCode.Double:
					return dval == convertible2.ToDouble (null);
				default:
					break;
				}
				break;
			default:
				break;
			}
			return object.Equals (this, obj);
		}

		public override int GetHashCode ()
		{
			// TODO: GetHashCode
			return base.GetHashCode ();
		}

		#region Numeric Comparsions

		// TODO: need to complete this

		public int CompareTo (double other)
		{
			var convertible = Value as IConvertible;
			if (convertible == null)
				throw new StonVariantException ();
			switch (convertible.GetTypeCode ()) {
			case TypeCode.SByte:
			case TypeCode.Byte:
			case TypeCode.Int16:
			case TypeCode.UInt16:
			case TypeCode.Int32:
			case TypeCode.UInt32:
			case TypeCode.Int64:
				return convertible.ToInt64 (null).CompareTo2 (other);
			case TypeCode.UInt64:
				return convertible.ToUInt64 (null).CompareTo2 (other);
			case TypeCode.Single:
			case TypeCode.Double:
				return convertible.ToDouble (null).CompareTo (other);
			default:
				throw new StonVariantException ();
			}
		}
		public int CompareTo (ulong other)
		{
			var convertible = Value as IConvertible;
			if (convertible == null)
				throw new StonVariantException ();
			switch (convertible.GetTypeCode ()) {
			case TypeCode.SByte:
			case TypeCode.Byte:
			case TypeCode.Int16:
			case TypeCode.UInt16:
			case TypeCode.Int32:
			case TypeCode.UInt32:
			case TypeCode.Int64:
				return convertible.ToInt64 (null).CompareTo2 (other);
			case TypeCode.UInt64:
				return convertible.ToUInt64 (null).CompareTo (other);
			case TypeCode.Single:
			case TypeCode.Double:
				return convertible.ToDouble (null).CompareTo2 (other);
			default:
				throw new StonVariantException ();
			}
		}
		public int CompareTo (long other)
		{
			var convertible = Value as IConvertible;
			if (convertible == null)
				throw new StonVariantException ();
			switch (convertible.GetTypeCode ()) {
			case TypeCode.SByte:
			case TypeCode.Byte:
			case TypeCode.Int16:
			case TypeCode.UInt16:
			case TypeCode.Int32:
			case TypeCode.UInt32:
			case TypeCode.Int64:
				return convertible.ToInt64 (null).CompareTo (other);
			case TypeCode.UInt64:
				return convertible.ToUInt64 (null).CompareTo2 (other);
			case TypeCode.Single:
			case TypeCode.Double:
				return convertible.ToDouble (null).CompareTo2 (other);
			default:
				throw new StonVariantException ();
			}
		}
		public static bool operator <  (StonVariant x, double y) { return x.CompareTo(y) < 0; }
		public static bool operator >  (StonVariant x, double y) { return x.CompareTo(y) > 0; }
		public static bool operator <= (StonVariant x, double y) { return x.CompareTo(y) <= 0; }
		public static bool operator >= (StonVariant x, double y) { return x.CompareTo(y) >= 0; }
		public static bool operator == (StonVariant x, double y) { return x.CompareTo(y) == 0; }
		public static bool operator != (StonVariant x, double y) { return x.CompareTo(y) != 0; }
		public static bool operator <  (StonVariant x, long y) { return x.CompareTo(y) < 0; }
		public static bool operator >  (StonVariant x, long y) { return x.CompareTo(y) > 0; }
		public static bool operator <= (StonVariant x, long y) { return x.CompareTo(y) <= 0; }
		public static bool operator >= (StonVariant x, long y) { return x.CompareTo(y) >= 0; }
		public static bool operator == (StonVariant x, long y) { return x.CompareTo(y) == 0; }
		public static bool operator != (StonVariant x, long y) { return x.CompareTo(y) != 0; }
		public static bool operator <  (StonVariant x, ulong y) { return x.CompareTo(y) < 0; }
		public static bool operator >  (StonVariant x, ulong y) { return x.CompareTo(y) > 0; }
		public static bool operator <= (StonVariant x, ulong y) { return x.CompareTo(y) <= 0; }
		public static bool operator >= (StonVariant x, ulong y) { return x.CompareTo(y) >= 0; }
		public static bool operator == (StonVariant x, ulong y) { return x.CompareTo(y) == 0; }
		public static bool operator != (StonVariant x, ulong y) { return x.CompareTo(y) != 0; }
		#endregion

		#region Binary Comparsions

		public static bool operator == (StonVariant x, byte[] y) { throw new NotImplementedException (); }
		public static bool operator != (StonVariant x, byte[] y) { throw new NotImplementedException (); }

		#endregion
		// TODO: binary comparsion
	}

	public class StonVariantException: Exception
	{
		public override string Message {
			get {
				return "Query condition evaluation failure.";
			}
		}
	}

	public class StaticStonVariant: StonVariant
	{
		readonly object obj;
		public StaticStonVariant(object val)
		{
			obj = val;
		}
		public override object Value {
			get {
				return obj;
			}
		}
	}

	public class SerializedStonVariant: StonVariant
	{
		readonly StonReader reader;
		readonly SerializedStonVariant parent;
		readonly int indexInParent;
		readonly string keyInParent;

		static readonly StonSerializer defaultSerializer = new StonSerializer();

		object value;
		bool isValueCached = false;

		public SerializedStonVariant(StonReader reader)
		{
			this.reader = reader;
			if (reader == null)
				throw new ArgumentNullException ("reader");
		}

		SerializedStonVariant(SerializedStonVariant parent, int index):
		this(parent.reader)
		{
			this.parent = parent;
			indexInParent = index;
			keyInParent = null;
		}
		SerializedStonVariant(SerializedStonVariant parent, string key):
		this(parent.reader)
		{
			this.parent = parent;
			indexInParent = 0;
			keyInParent = key;
		}

		public void Locate(int index)
		{
			LocateSelf ();
		
			if (reader.CurrentNodeType != StonReader.NodeType.List) {
				throw new StonException ();
			}

			reader.StartList ();
			while (index > 0) {
				reader.Skip ();
				--index;
			}
		}

		public void Locate(string key)
		{
			LocateSelf();

			if (reader.CurrentNodeType != StonReader.NodeType.Dictionary) {
				throw new StonException ();
			}

			reader.StartDictionary ();
			while (reader.CurrentNodeType != StonReader.NodeType.EndOfCollection &&
				!reader.CurrentDictionaryKey.Equals(key)) {
				reader.Skip ();
			}

			// Key not found...
			throw new StonException ();
		}

		public void LocateSelf()
		{
			if (parent == null) {
				reader.Reset ();
			} else if (keyInParent != null) {
				parent.Locate (indexInParent);
			} else {
				parent.Locate (keyInParent);
			}
		}

		public override object Value {
			get {
				if (!isValueCached) {
					LocateSelf ();
					value = defaultSerializer.DeserializeObject (reader);
					isValueCached = true;
				}
				return value;
			}
		}

	}
}


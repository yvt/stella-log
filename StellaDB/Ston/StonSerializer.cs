using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB.Ston
{
	public class StonSerializer
	{
		static class DataTypes
		{
			public const byte Null = 0x00;
			public const byte Map = 0x01;
			public const byte List = 0x02;
			public const byte String8 = 0x03;
			public const byte String16 = 0x04;
			public const byte String24 = 0x05;
			public const byte String32 = 0x06;
			public const byte PositiveInteger8 = 0x0b;
			public const byte PositiveInteger16 = 0x0c;
			public const byte PositiveInteger24 = 0x0d;
			public const byte PositiveInteger32 = 0x0e;
			public const byte PositiveInteger48 = 0x0f;
			public const byte PositiveInteger64 = 0x10;
			public const byte NegativeInteger8 = 0x11;
			public const byte NegativeInteger16 = 0x12;
			public const byte NegativeInteger24 = 0x13;
			public const byte NegativeInteger32 = 0x14;
			public const byte NegativeInteger48 = 0x15;
			public const byte NegativeInteger64 = 0x16;
			public const byte True = 0x17;
			public const byte False = 0x18;
			public const byte Float = 0x19;
			public const byte Double = 0x1a;
			public const byte DateTime = 0x1b;
			public const byte ZeroInt = 0x1d;
			public const byte EmptyString = 0x1e;
			public const byte EOMLMarker = 0x1f; // end of list/map
			public const byte Char8 = 0x20;
			public const byte Char16 = 0x21;
			public const byte StringifiedSignedInteger8 = 0x22;
			public const byte StringifiedInteger8 = 0x23;
			public const byte StringifiedInteger16 = 0x24;
			public const byte StringifiedInteger32 = 0x25;
			public const byte StringifiedInteger64 = 0x26;
			public const byte IntegerBase = 0x80; // above this are 7-bit signed integers
		}

		Dictionary<Type, StonConverter> converters = new Dictionary<Type, StonConverter>();

		/// <summary>
		/// To be added.
		/// </summary>
		/// <value>The recursion limit.</value>
		public int RecursionLimit { get; set; }

		public StonSerializer ()
		{
			RecursionLimit = 100;
		}

		public object ConvertToType(object obj, Type type)
		{
			if (obj == null) {
				return null;
			}
			if (type == null) {
				return obj;
			} else {
				if (type.IsInstanceOfType(obj)) {
					return obj;
				}

				var dic = obj as IDictionary<string, object>;
				if (dic != null) {

					StonConverter converter;
					if (converters.TryGetValue (type, out converter)) {
						return converter.Deserialize (dic, type, this);
					}

					if (type.IsGenericType && !type.IsGenericTypeDefinition &&
						!type.Equals(typeof(IDictionary<string, object>))) {
						var definition = type.GetGenericTypeDefinition ();
						if (definition.Equals(typeof(IDictionary<,>))) {
							var param = type.GetGenericArguments ();
							if (param[0] == typeof(string)) {
								var etype = param [1];
								var colType = typeof(Dictionary<,>).MakeGenericType (typeof(string), param[1]);
								var inst = colType.GetConstructor(new Type[]{}).Invoke(null);
								var addfn = colType.GetMethod("Add", colType.GetGenericArguments());
								foreach (var e in dic) {
									addfn.Invoke(inst,
										new object[] {e.Key,
											ConvertToType(e.Value, etype)});
								}
								return inst;
							}
						}
					}
				}

				var enumerable = obj as IEnumerable<object>;
				if (enumerable != null) {
					if (type.IsGenericType && !type.IsGenericTypeDefinition &&
						!type.Equals(typeof(IList<object>)) &&
						!type.Equals(typeof(IEnumerable<object>)) &&
						!type.Equals(typeof(ICollection<object>))) {
						var definition = type.GetGenericTypeDefinition ();
						if (definition.Equals(typeof(IList<>)) ||
							definition.Equals(typeof(IEnumerable<>)) ||
							definition.Equals(typeof(ICollection<>))) {
							var etype = type.GetGenericArguments ()[0];
							var listType = typeof(List<>).MakeGenericType (etype);
							var inst = listType.GetConstructor(new Type[]{}).Invoke(null);
							var addfn = listType.GetMethod("Add", new Type[]{etype});
							foreach (var e in enumerable) {
								addfn.Invoke(inst,
									new object[] {ConvertToType(e, etype)});
							}
							return inst;
						}
					}
				}

				return Convert.ChangeType (obj, type);
			}

		}

		public T ConvertToType<T>(object obj)
		{
			return (T)ConvertToType (obj, typeof(T));
		}

		public void RegisterConverters(IEnumerable<StonConverter> converter)
		{
			foreach (var c in converter) {
				foreach (var type in c.SupportedTypes) {
					converters [type] = c;
				}
			}
		}

		IEnumerable<object> DeserializeList(Utils.MemoryBinaryReader reader, int level)
		{
			var ret = new List<object> ();
			while (reader.GetBuffer()[reader.Position] != DataTypes.EOMLMarker) {
				ret.Add (DeserializeImpl (reader, level + 1));
			}
			reader.ReadUInt8 ();
			return ret;
		}
		IDictionary<string, object> DeserializeMap(Utils.MemoryBinaryReader reader, int level)
		{
			var ret = new Dictionary<string, object> ();
			while (reader.GetBuffer()[reader.Position] != DataTypes.EOMLMarker) {
				var key = DeserializeImpl (reader, level + 1) as string;
				var val = DeserializeImpl (reader, level + 1);
				if (key == null) {
					throw new InvalidOperationException ("Key must be a string.");
				}
				ret.Add (key, val);
			}
			reader.ReadUInt8 ();
			return ret;
		}
		string DeserializeString(byte type, Utils.MemoryBinaryReader reader)
		{
			uint numBytes = 0;
			try {
				checked {
					switch (type) {
					case DataTypes.EmptyString:
						numBytes = 0;
						break;
					case DataTypes.String8:
						numBytes = reader.ReadUInt8 ();
						numBytes += 1;
						break;
					case DataTypes.String16:
						numBytes = reader.ReadUInt8 ();
						numBytes |= (uint)(reader.ReadUInt8 ()) << 8;
						numBytes += 1 + 0x100;
						break;
					case DataTypes.String24:
						numBytes = reader.ReadUInt8 ();
						numBytes |= (uint)(reader.ReadUInt8 ()) << 8;
						numBytes |= (uint)(reader.ReadUInt8 ()) << 16;
						numBytes += 1 + 0x10100;
						break;
					case DataTypes.String32:
						numBytes = reader.ReadUInt8 ();
						numBytes |= (uint)(reader.ReadUInt8 ()) << 8;
						numBytes |= (uint)(reader.ReadUInt8 ()) << 16;
						numBytes |= (uint)(reader.ReadUInt8 ()) << 24;
						numBytes += 1 + 0x1010100;
						break;
					}
				}
				if (numBytes > (uint)int.MaxValue) {
					throw new OverflowException ();
				}
			} catch (OverflowException ex) {
				throw new StonException ("String length is too long.", ex);
			}

			byte[] buffer = new byte[numBytes];
			reader.Read (buffer, 0, buffer.Length);
			return utf8.GetString (buffer);
		}
		int DeserializeSmallInteger(byte type, Utils.MemoryBinaryReader reader)
		{
			// Sign extend
			int v = (int)(type & 0x7f) << (32 - 7);;
			v >>= (32 - 7);
			if (v >= 0)
				++v;
			return v;
		}
		ulong DeserializePositiveInteger(byte type, Utils.MemoryBinaryReader reader)
		{
			ulong val = 0;
			try {
				checked {
					switch (type) {
					case DataTypes.PositiveInteger8:
						val = reader.ReadUInt8();
						val += 65;
						break;
					case DataTypes.PositiveInteger16:
						val = reader.ReadUInt8();
						val |= (ulong)reader.ReadUInt8() << 8;
						val += 65 + 0x100;
						break;
					case DataTypes.PositiveInteger24:
						val = reader.ReadUInt8();
						val |= (ulong)reader.ReadUInt8() << 8;
						val |= (ulong)reader.ReadUInt8() << 16;
						val += 65 + 0x10100;
						break;
					case DataTypes.PositiveInteger32:
						val = reader.ReadUInt32();
						val += 65 + 0x1010100;
						break;
					case DataTypes.PositiveInteger48:
						val = reader.ReadUInt32();
						val |= (ulong)reader.ReadUInt16() << 32;
						val += 65 + 0x101010100;
						break;
					case DataTypes.PositiveInteger64:
						val = reader.ReadUInt64();
						val += 65 + 0x1000101010100;
						break;
					}
				}
				return val;
			} catch (OverflowException ex) {
				throw new StonException ("Integer value could not be represented in 64bit.", ex);
			}
		}
		long DeserializeNegativeInteger(byte type, Utils.MemoryBinaryReader reader)
		{
			ulong val = 0;
			try {
				checked {
					switch (type) {
					case DataTypes.NegativeInteger8:
						val = reader.ReadUInt8();
						break;
					case DataTypes.NegativeInteger16:
						val = reader.ReadUInt8();
						val |= (ulong)reader.ReadUInt8() << 8;
						val += 0x100;
						break;
					case DataTypes.NegativeInteger24:
						val = reader.ReadUInt8();
						val |= (ulong)reader.ReadUInt8() << 8;
						val |= (ulong)reader.ReadUInt8() << 16;
						val += 0x10100;
						break;
					case DataTypes.NegativeInteger32:
						val = reader.ReadUInt32();
						val += 0x1010100;
						break;
					case DataTypes.NegativeInteger48:
						val = reader.ReadUInt32();
						val |= (ulong)reader.ReadUInt16() << 32;
						val += 0x101010100;
						break;
					case DataTypes.NegativeInteger64:
						val = reader.ReadUInt64();
						val += 0x1000101010100;
						break;
					}
				}
				return -65 - (long)val;
			} catch (OverflowException ex) {
				throw new StonException ("Integer value could not be represented in 64bit.", ex);
			}
		}
		float DeserializeFloat(Utils.MemoryBinaryReader reader)
		{
			throw new NotImplementedException ("Deserializing float is not supported yet.");
		}
		double DeserializeDouble(Utils.MemoryBinaryReader reader)
		{
			return BitConverter.Int64BitsToDouble (reader.ReadInt64 ());
		}
		char DeserializeChar(byte type, Utils.MemoryBinaryReader reader)
		{
			switch (type) {
			case DataTypes.Char8:
				return (char)reader.ReadUInt8 ();
			case DataTypes.Char16:
				return (char)reader.ReadUInt16 ();
			}
			throw new InvalidOperationException ();
		}
		string DeserializeStringifiedInteger(byte type, Utils.MemoryBinaryReader reader)
		{
			if (type == DataTypes.StringifiedSignedInteger8) {
				int v = reader.ReadInt8 ();
				return v.ToString ();
			}
			ulong val = 0;
			checked {
				switch (type) {
				case DataTypes.StringifiedInteger8:
					val = reader.ReadUInt8 ();
					break;
				case DataTypes.StringifiedInteger16:
					val = reader.ReadUInt16 ();
					val += 0x100;
					break;
				case DataTypes.StringifiedInteger32:
					val = reader.ReadUInt32 ();
					val += 0x10100;
					break;
				case DataTypes.StringifiedInteger64:
					val = reader.ReadUInt64 ();
					val += 0x100010100;
					break;
				}
				val += 128;
			}
			return val.ToString ();
		}
		object DeserializeImpl(Utils.MemoryBinaryReader reader, int level = 0)
		{
			byte type = reader.ReadUInt8 ();
			switch (type) {
			case DataTypes.Null:
				return null;
			case DataTypes.Map:
				return DeserializeMap (reader, level);
			case DataTypes.List:
				return DeserializeList (reader, level);
			case DataTypes.PositiveInteger8:
			case DataTypes.PositiveInteger16:
			case DataTypes.PositiveInteger24:
			case DataTypes.PositiveInteger32:
			case DataTypes.PositiveInteger48:
			case DataTypes.PositiveInteger64:
				return DeserializePositiveInteger (type, reader);
			case DataTypes.NegativeInteger8:
			case DataTypes.NegativeInteger16:
			case DataTypes.NegativeInteger24:
			case DataTypes.NegativeInteger32:
			case DataTypes.NegativeInteger48:
			case DataTypes.NegativeInteger64:
				return DeserializeNegativeInteger (type, reader);
			case DataTypes.True:
				return true;
			case DataTypes.False:
				return false;
			case DataTypes.Float:
				return DeserializeFloat (reader);
			case DataTypes.Double:
				return DeserializeDouble (reader);
			case DataTypes.DateTime:
				throw new NotImplementedException ("Deserializing DateTime is not supported yet.");
			case DataTypes.ZeroInt:
				return 0;
			case DataTypes.EmptyString:
			case DataTypes.String8:
			case DataTypes.String16:
			case DataTypes.String24:
			case DataTypes.String32:
				return DeserializeString (type, reader);
			case DataTypes.EOMLMarker:
				throw new StonException ("Unexpected end of list/map marker was found.");
			case DataTypes.Char8:
			case DataTypes.Char16:
				return DeserializeChar (type, reader);
			case DataTypes.StringifiedSignedInteger8:
			case DataTypes.StringifiedInteger8:
			case DataTypes.StringifiedInteger16:
			case DataTypes.StringifiedInteger32:
			case DataTypes.StringifiedInteger64:
				return DeserializeStringifiedInteger (type, reader);
			default:
				if (type >= DataTypes.IntegerBase) {
					return DeserializeSmallInteger (type, reader);
				} else {
					throw new StonException (
						string.Format ("Unknown data type {0}.", type));
				}
			}
		}

		public object DeserializeObject(byte[] buffer)
		{
			if (buffer == null)
				throw new ArgumentNullException ("buffer");
			return DeserializeImpl (new Utils.MemoryBinaryReader (buffer, buffer.Length));
		}

		public object Deserialize(byte[] buffer, Type type)
		{
			return ConvertToType (DeserializeObject (buffer), type);
		}

		public T Deserialize<T>(byte[] buffer)
		{
			return (T)Deserialize (buffer, typeof(T));
		}

		void SerializeNull(Utils.MemoryBinaryWriter bw)
		{
			bw.Write ((byte)0x00);
		}
		void SerializeString(byte[] s, Utils.MemoryBinaryWriter bw)
		{
			if (s.Length == 0) {
				bw.Write (DataTypes.EmptyString);
			} else if (s.Length <= 0x100) {
				bw.Write ((byte)DataTypes.String8);
				bw.Write ((byte)(s.Length - 1));
			} else if (s.Length <= 0x10100) {
				bw.Write ((byte)DataTypes.String16);
				int l = s.Length - 0x101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
			} else if (s.Length <= 0x1010100) {
				bw.Write ((byte)DataTypes.String24);
				int l = s.Length - 0x10101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
				bw.Write ((byte)(l >> 16));
			} else {
				bw.Write ((byte)DataTypes.String32);
				int l = s.Length - 0x1010101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
				bw.Write ((byte)(l >> 16));
				bw.Write ((byte)(l >> 24));
			}
			bw.Write (s);
		}
		static bool IsPossiblyStringifiedInteger(string s)
		{
			for(int i = 0, count = s.Length; i < count; ++i) {
				var c = s [i];
				if (c >= '0' && c <= '9') {
					continue;
				} else if (c == '-' && i == 0) {
					continue;
				} else {
					return false;
				}
			}
			return true;
		}
		static readonly System.Text.Encoding utf8 = new System.Text.UTF8Encoding ();
		void SerializeString(string s, Utils.MemoryBinaryWriter bw)
		{
			// See if this can be encoded as stringified integer
			if (s.Length > 0 && IsPossiblyStringifiedInteger(s)) {
				ulong lval;
				if (s[0] != '-' && ulong.TryParse(s, out lval) && lval.ToString().Equals(s)) {
					if (lval < 128) {
						bw.Write (DataTypes.StringifiedSignedInteger8);
						bw.Write ((byte)(lval));
						return;
					}
					lval -= 128;
					if (lval < 0x100) {
						bw.Write (DataTypes.StringifiedInteger8);
						bw.Write ((byte)(lval));
					} else if (lval < 0x10100) {
						lval -= 0x100;
						bw.Write (DataTypes.StringifiedInteger16);
						bw.Write ((byte)(lval));
						bw.Write ((byte)(lval >> 8));
					} else if (lval < 0x100010100) {
						lval -= 0x10100;
						bw.Write (DataTypes.StringifiedInteger32);
						bw.Write ((byte)(lval));
						bw.Write ((byte)(lval >> 8));
						bw.Write ((byte)(lval >> 16));
						bw.Write ((byte)(lval >> 24));
					} else{
						lval -= 0x100010100;
						bw.Write (DataTypes.StringifiedInteger64);
						bw.Write (lval);
					}
					return;
				}

				sbyte sval;
				if (sbyte.TryParse(s, out sval) && sval.ToString().Equals(s)) {
					bw.Write (DataTypes.StringifiedSignedInteger8);
					bw.Write ((byte)(sval));
					return;
				}
			}

			SerializeString(utf8.GetBytes(s), bw);
		}
		void SerializeInteger(long v, Utils.MemoryBinaryWriter bw)
		{
			if (v == 0) {
				bw.Write (DataTypes.ZeroInt);
				return;
			} else if (v >= -64 && v < 0) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)v & 0x7f)));
				return;
			} else if (v > 0 && v <= 64) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)(v - 1) & 0x7f)));
				return;
			}
			if (v > 0) {
				v -= 65;
				if (v < 0x100) {
					bw.Write (DataTypes.PositiveInteger8);
					bw.Write ((byte)(v));
				} else if (v < 0x10100) {
					v -= 0x100;
					bw.Write (DataTypes.PositiveInteger16);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
				} else if (v < 0x1010100) {
					v -= 0x10100;
					bw.Write (DataTypes.PositiveInteger24);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
				} else if (v < 0x101010100) {
					v -= 0x1010100;
					bw.Write (DataTypes.PositiveInteger32);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
					bw.Write ((byte)(v >> 24));
				} else if (v < 0x1000101010100) {
					v -= 0x101010100;
					bw.Write (DataTypes.PositiveInteger48);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
					bw.Write ((byte)(v >> 24));
					bw.Write ((byte)(v >> 32));
					bw.Write ((byte)(v >> 40));
				} else {
					v -= 0x1000101010100;
					bw.Write (DataTypes.PositiveInteger64);
					bw.Write (v);
				}
			} else {
				v = -65 - v;
				if (v < 0x100) {
					bw.Write (DataTypes.NegativeInteger8);
					bw.Write ((byte)(v));
				} else if (v < 0x10100) {
					v -= 0x100;
					bw.Write (DataTypes.NegativeInteger16);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
				} else if (v < 0x1010100) {
					v -= 0x10100;
					bw.Write (DataTypes.NegativeInteger24);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
				} else if (v < 0x101010100) {
					v -= 0x1010100;
					bw.Write (DataTypes.NegativeInteger32);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
					bw.Write ((byte)(v >> 24));
				} else if (v < 0x1000101010100) {
					v -= 0x101010100;
					bw.Write (DataTypes.NegativeInteger48);
					bw.Write ((byte)(v));
					bw.Write ((byte)(v >> 8));
					bw.Write ((byte)(v >> 16));
					bw.Write ((byte)(v >> 24));
					bw.Write ((byte)(v >> 32));
					bw.Write ((byte)(v >> 40));
				} else {
					v -= 0x1000101010100;
					bw.Write (DataTypes.NegativeInteger64);
					bw.Write (v);
				}
			}
		}
		void SerializeInteger(ulong v, Utils.MemoryBinaryWriter bw)
		{
			if (v == 0) {
				bw.Write (DataTypes.ZeroInt);
				return;
			} else if (v > 0 && v <= 64) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)(v - 1) & 0x7f)));
				return;
			}
			v -= 65;
			if (v < 0x100) {
				bw.Write (DataTypes.PositiveInteger8);
				bw.Write ((byte)(v));
			} else if (v < 0x10100) {
				v -= 0x100;
				bw.Write (DataTypes.PositiveInteger16);
				bw.Write ((byte)(v));
				bw.Write ((byte)(v >> 8));
			} else if (v < 0x1010100) {
				v -= 0x10100;
				bw.Write (DataTypes.PositiveInteger24);
				bw.Write ((byte)(v));
				bw.Write ((byte)(v >> 8));
				bw.Write ((byte)(v >> 16));
			} else if (v < 0x101010100) {
				v -= 0x1010100;
				bw.Write (DataTypes.PositiveInteger32);
				bw.Write ((byte)(v));
				bw.Write ((byte)(v >> 8));
				bw.Write ((byte)(v >> 16));
				bw.Write ((byte)(v >> 24));
			} else if (v < 0x1000101010100) {
				v -= 0x101010100;
				bw.Write (DataTypes.PositiveInteger48);
				bw.Write ((byte)(v));
				bw.Write ((byte)(v >> 8));
				bw.Write ((byte)(v >> 16));
				bw.Write ((byte)(v >> 24));
				bw.Write ((byte)(v >> 32));
				bw.Write ((byte)(v >> 40));
			} else {
				v -= 0x1000101010100;
				bw.Write (DataTypes.PositiveInteger64);
				bw.Write (v);
			}
		}
		void SerializeChar(char c, Utils.MemoryBinaryWriter bw)
		{
			var utf16 = (ushort)c;
			if (utf16 < 0x100) {
				bw.Write (DataTypes.Char8);
				bw.Write ((byte)utf16);
			} else {
				bw.Write (DataTypes.Char16);
				bw.Write ((byte)utf16);
				bw.Write ((byte)(utf16 >> 8));
			}
		}
		void SerializeBool(bool v, Utils.MemoryBinaryWriter bw)
		{
			bw.Write (v ? DataTypes.True : DataTypes.False);
		}
		void SerializeFloat(float f, Utils.MemoryBinaryWriter bw)
		{
			throw new NotImplementedException ("Serializing float is not implemented yet.");
		}
		void SerializeDouble(double f, Utils.MemoryBinaryWriter bw)
		{
			bw.Write (DataTypes.Double);
			bw.Write (BitConverter.DoubleToInt64Bits (f));
		}
		void SerializeMap(IEnumerable<KeyValuePair<string, object>> map, 
			Utils.MemoryBinaryWriter bw, int level)
		{
			bw.Write (DataTypes.Map);
			foreach (var e in map) {
				SerializeString (e.Key, bw);
				SerializeImpl (e.Value, bw, level + 1);
			}
			bw.Write (DataTypes.EOMLMarker);
		}
		void SerializeList<T>(IEnumerable<T> list, Utils.MemoryBinaryWriter bw, int level)
		{
			bw.Write (DataTypes.List);
			foreach (var e in list) {
				SerializeImpl (e, bw, level + 1);
			}
			bw.Write (DataTypes.EOMLMarker);
		}

		void SerializeImpl(object obj, Utils.MemoryBinaryWriter bw, int level)
		{
			if (level > RecursionLimit) {
				throw new InvalidOperationException ("Recursion limit reached.");
			}

			if (obj == null) {
				SerializeNull (bw);
			} else {
				switch (Convert.GetTypeCode (obj)) {
				case TypeCode.Empty:
				case TypeCode.DBNull:
					SerializeNull (bw);
					break;
				case TypeCode.Boolean:
					SerializeBool (Convert.ToBoolean (obj), bw);
					break;
				case TypeCode.Char:
					SerializeChar (Convert.ToChar (obj), bw);
					break;
				case TypeCode.String:
					SerializeString (Convert.ToString (obj), bw);
					break;
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
					SerializeInteger (Convert.ToInt64 (obj), bw);
					break;
				case TypeCode.UInt64:
					SerializeInteger (Convert.ToUInt64 (obj), bw);
					break;
				case TypeCode.Single:
					SerializeFloat (Convert.ToSingle (obj), bw);
					break;
				case TypeCode.Double:
					SerializeDouble (Convert.ToDouble (obj), bw);
					break;
				case TypeCode.Decimal:
					throw new InvalidOperationException (
						string.Format("Unserializable value {0} which is Decimal.",
							obj));
				case TypeCode.DateTime:
					throw new NotImplementedException ("Serializing DateTime is not implemented yet.");
				case TypeCode.Object:
					var dic = obj as IEnumerable<KeyValuePair<string, object>>;
					var enumerable = obj as IEnumerable<object>;
					if (enumerable != null) {
						SerializeList<object> (enumerable, bw, level);
						break;
					} 
					if (dic == null) {
						StonConverter cvt;
						if (converters.TryGetValue(obj.GetType(), out cvt)) {
							dic = cvt.Serialize (obj, this);
						} else {
							// might be arbitary IDictionary<string, T> where T != object.
							// note that IDictionary<string, T> is not compatible with
							// IDictionary<string, object>.
							ICovariantDictionaryWrapper wrapper;
							if (covarianceWrapper.TryGetValue(obj.GetType(), out wrapper)) {
								dic = wrapper.Convert (obj);
							} else {
								foreach (var i in obj.GetType().GetInterfaces()) {
									if (i.IsGenericType && 
										i.GetGenericTypeDefinition() == typeof(IDictionary<,>)) {
										var param = i.GetGenericArguments ();
										if (param[0] == typeof(string)) {
											var wrapperType = typeof(CovariantDictionaryWrapper<>);
											wrapperType = wrapperType.MakeGenericType (param [1]);
											wrapper = (ICovariantDictionaryWrapper)
												wrapperType.GetConstructor (new Type[]{ }).Invoke (null);
											covarianceWrapper.Add (obj.GetType (), wrapper);
											dic = wrapper.Convert (obj);
										}
									}
								}
							}
						}
					}
					SerializeMap (dic, bw, level);
					break;
				default:
					throw new InvalidOperationException ();
				}
			}
		}

		internal void Serialize(object obj, Utils.MemoryBinaryWriter bw)
		{
			SerializeImpl (obj, bw, 0);
		}

		public byte[] Serialize(object obj)
		{
			var bw = new Utils.MemoryBinaryWriter ();
			Serialize (obj, bw);
			return bw.ToArray ();
		}

		Dictionary<Type, ICovariantDictionaryWrapper> covarianceWrapper
			= new Dictionary<Type, ICovariantDictionaryWrapper>();

		// We need this because KeyValuePair is not KeyValuePair<out TKey, out TValue>
		interface ICovariantDictionaryWrapper
		{
			IEnumerable<KeyValuePair<string, object>> Convert (object inp);
		}
		class CovariantDictionaryWrapper<TValue>: ICovariantDictionaryWrapper
		{
			public IEnumerable<KeyValuePair<string, object>> Convert (object inp)
			{
				var src = (IEnumerable<KeyValuePair<string, TValue>>)inp;
				return from item in src
						select new KeyValuePair<string, object> (item.Key, item.Value);
			}
		}
	}
}


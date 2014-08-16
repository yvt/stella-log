using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;

namespace Yavit.StellaDB.Ston
{
	public sealed class StonReader
	{
		public enum NodeType
		{
			EndOfDocument,

			Null,
			Dictionary,
			List,
			EndOfCollection,
			Integer,
			Float,
			Double,
			Boolean,
			Char,
			String
		}
		readonly Utils.MemoryBinaryReader reader;

		enum State
		{
			Scalar,
			MapValue,
			ArrayElement,
			EndOfMap,
			EndOfArray
		}

		static System.Text.Encoding utf8 = new System.Text.UTF8Encoding();

		readonly Stack<State> state = new Stack<State>();
		String currentDictionaryKey;

		public StonReader (byte[] bytes)
		{
			if (bytes == null)
				throw new ArgumentNullException ("bytes");

			reader = new Utils.MemoryBinaryReader (bytes, bytes.Length);
			state.Push (State.Scalar);
		}
		internal StonReader(Utils.MemoryBinaryReader reader)
		{
			if (reader == null)
				throw new ArgumentNullException ("reader");
			this.reader = reader;
			state.Push (State.Scalar);
		}

		public void Reset()
		{
			reader.Position = 0;
			state.Clear ();
			state.Push (State.Scalar);
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		void CheckNotEndOfDocument()
		{
			if (state.Count == 0) {
				throw new InvalidOperationException ("End of document.");
			}
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		void CheckNotEndOfCollection()
		{
			var st = state.Peek ();
			if (st == State.EndOfMap || st == State.EndOfArray) {
				throw new InvalidOperationException ("End of collection.");
			}
		}

		void ThrowInvalidDataType()
		{
			throw new InvalidOperationException(string.Format("Mismatch between called method and the actual node type({0}).", CurrentNodeType));
		}

		void ValueRead()
		{
			switch (state.Pop()) {
			case State.Scalar:
				break;
			case State.ArrayElement:
				if (DataType == DataTypes.EOMLMarker) {
					state.Push (State.EndOfArray);
				} else {
					state.Push (State.ArrayElement);
				}
				break;
			case State.MapValue:
				if (DataType == DataTypes.EOMLMarker) {
					state.Push (State.EndOfMap);
				} else {
					state.Push (State.MapValue);
					currentDictionaryKey = InternalReadString (true);
				}
				break;
			}
		}

		byte DataType
		{
			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			get {
				return reader.GetBuffer () [reader.Position];
			}
		}

		public NodeType CurrentNodeType
		{
			get {
				if (state.Count == 0) {
					return NodeType.EndOfDocument;
				}
				var type = DataType;
				switch (type) {
				case DataTypes.Null:
					return NodeType.Null;
				case DataTypes.Map:
					return NodeType.Dictionary;
				case DataTypes.List:
					return NodeType.List;
				case DataTypes.PositiveInteger8:
				case DataTypes.PositiveInteger16:
				case DataTypes.PositiveInteger24:
				case DataTypes.PositiveInteger32:
				case DataTypes.PositiveInteger48:
				case DataTypes.PositiveInteger64:
				case DataTypes.NegativeInteger8:
				case DataTypes.NegativeInteger16:
				case DataTypes.NegativeInteger24:
				case DataTypes.NegativeInteger32:
				case DataTypes.NegativeInteger48:
				case DataTypes.NegativeInteger64:
				case DataTypes.ZeroInt:
					return NodeType.Integer;
				case DataTypes.True:
				case DataTypes.False:
					return NodeType.Boolean;
				case DataTypes.Float:
					return NodeType.Float;
				case DataTypes.Double:
					return NodeType.Double;
				case DataTypes.DateTime:
					throw new NotImplementedException ("Deserializing DateTime is not supported yet.");
				case DataTypes.EmptyString:
				case DataTypes.String8:
				case DataTypes.String16:
				case DataTypes.String24:
				case DataTypes.String32:
				case DataTypes.StringifiedSignedInteger8:
				case DataTypes.StringifiedInteger8:
				case DataTypes.StringifiedInteger16:
				case DataTypes.StringifiedInteger32:
				case DataTypes.StringifiedInteger64:
					return NodeType.String;
				case DataTypes.EOMLMarker:
					var st = state.Peek ();
					if (st != State.EndOfArray && st != State.EndOfMap) {
						throw new StonException ("Unexpected end of map/list marker.");
					}
					return NodeType.EndOfCollection;
				case DataTypes.Char8:
				case DataTypes.Char16:
					return NodeType.Char;
				default:
					if (type >= DataTypes.IntegerBase) {
						return NodeType.Integer;
					} else {
						throw new StonException (
							string.Format ("Unknown data type {0}.", type));
					}
				}
			}
		}

		public String CurrentDictionaryKey
		{
			get {
				CheckNotEndOfDocument ();
				if (state.Peek() != State.MapValue) {
					throw new InvalidOperationException ("Not reading a dictionary item.");
				}
				return currentDictionaryKey;
			}
		}

		String InternalReadString(bool readingDictionaryKey)
		{
			int len = 1;
			switch (DataType) {
			case DataTypes.EmptyString:
				break;
			case DataTypes.StringifiedSignedInteger8:
			case DataTypes.StringifiedInteger8:
				len += 1;
				break;
			case DataTypes.StringifiedInteger16:
				len += 2;
				break;
			case DataTypes.StringifiedInteger32:
				len += 4;
				break;
			case DataTypes.StringifiedInteger64:
				len += 8;
				break;
			case DataTypes.String8:
			case DataTypes.String16:
			case DataTypes.String24:
			case DataTypes.String32:
				uint numBytes = 0;
				var buffer = reader.GetBuffer ();
				int offs = reader.Position + 1;
				checked {
					switch (DataType) {
					case DataTypes.EmptyString:
						numBytes = 0;
						break;
					case DataTypes.String8:
						numBytes = buffer [offs++];
						numBytes += 1;
						break;
					case DataTypes.String16:
						numBytes = buffer [offs++];
						numBytes |= (uint)buffer [offs++] << 8;
						numBytes += 1 + 0x100;
						break;
					case DataTypes.String24:
						numBytes = buffer [offs++];
						numBytes |= (uint)buffer [offs++] << 8;
						numBytes |= (uint)buffer [offs++] << 16;
						numBytes += 1 + 0x10100;
						break;
					case DataTypes.String32:
						numBytes = buffer [offs++];
						numBytes |= (uint)buffer [offs++] << 8;
						numBytes |= (uint)buffer [offs++] << 16;
						numBytes |= (uint)buffer [offs++] << 24;
						numBytes += 1 + 0x1010100;
						break;
					}
				}
				if (numBytes > (uint)int.MaxValue) {
					throw new OverflowException ();
				}
				len = checked(offs - reader.Position + (int)numBytes);
				break;
			}

			var position = reader.Position;
			reader.Position += len;
			if (!readingDictionaryKey)
				ValueRead ();
			return new String (reader.GetBuffer (), position);
		}

		public String ReadString()
		{
			return InternalReadString (false);
		}
		public StonInteger ReadInteger()
		{
			CheckNotEndOfDocument ();
			var type = DataType;
			switch (type) {
			case DataTypes.ZeroInt:
				return 0;
			case DataTypes.PositiveInteger8:
			case DataTypes.PositiveInteger16:
			case DataTypes.PositiveInteger24:
			case DataTypes.PositiveInteger32:
			case DataTypes.PositiveInteger48:
			case DataTypes.PositiveInteger64:
				return ReadPositiveInteger ();
			case DataTypes.NegativeInteger8:
			case DataTypes.NegativeInteger16:
			case DataTypes.NegativeInteger24:
			case DataTypes.NegativeInteger32:
			case DataTypes.NegativeInteger48:
			case DataTypes.NegativeInteger64:
				return ReadNegativeInteger ();
			default:
				if (type >= DataTypes.IntegerBase) {
					return ReadSmallInteger ();
				} else {
					ThrowInvalidDataType ();
					return 0;
				}
			}
		}

		public float ReadFloat()
		{
			throw new NotImplementedException ("Deserializing float is not supported yet.");
		}

		public double ReadDouble()
		{
			CheckNotEndOfDocument ();
			var type = DataType;
			if (type != DataTypes.Double) {
				ThrowInvalidDataType ();
			}
			reader.ReadUInt8 ();
			var r = BitConverter.Int64BitsToDouble (reader.ReadInt64 ());
			ValueRead ();
			return r;
		}

		public char ReadChar()
		{
			CheckNotEndOfDocument ();
			var type = DataType;
			char ret;
			switch (type) {
			case DataTypes.Char8:
				reader.ReadUInt8 ();
				ret = (char)reader.ReadUInt8 ();
				break;
			case DataTypes.Char16:
				reader.ReadUInt8 ();
				ret = (char)reader.ReadUInt16 ();
				break;
			default:
				ThrowInvalidDataType ();
				ret = (char)0;
				break;
			}
			ValueRead ();
			return ret;
		}

		public void ReadNull()
		{
			CheckNotEndOfDocument ();
			var type = DataType;
			if (type != DataTypes.Null)
				ThrowInvalidDataType ();
			reader.ReadUInt8 ();
			ValueRead ();
		}

		public bool ReadBoolean()
		{
			CheckNotEndOfDocument ();
			var type = DataType;
			switch (type) {
			case DataTypes.True:
				reader.ReadUInt8 ();
				ValueRead ();
				return true;
			case DataTypes.False:
				reader.ReadUInt8 ();
				ValueRead ();
				return false;
			default:
				ThrowInvalidDataType ();
				return false;
			}
		}

		int ReadSmallInteger()
		{
			// Sign extend
			int v = (int)(reader.ReadUInt8() & 0x7f) << (32 - 7);;
			v >>= (32 - 7);
			if (v >= 0)
				++v;
			ValueRead ();
			return v;
		}

		ulong ReadPositiveInteger()
		{
			ulong val = 0;
			var type = reader.ReadUInt8 ();
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
				ValueRead ();
				return val;
			} catch (OverflowException ex) {
				throw new StonException ("Integer value could not be represented in 64bit.", ex);
			}
		}
		long ReadNegativeInteger()
		{
			ulong val = 0;
			var type = reader.ReadUInt8 ();
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
				ValueRead ();
				return -65 - (long)val;
			} catch (OverflowException ex) {
				throw new StonException ("Integer value could not be represented in 64bit.", ex);
			}
		}

		public void StartDictionary()
		{
			CheckNotEndOfDocument ();
			if (DataType == DataTypes.Map) {
				reader.ReadUInt8 ();
				state.Push (State.MapValue);
				ValueRead ();
			} else {
				throw new InvalidOperationException ("Attempted to start reading a dictionary when dictionary is not found.");
			}
		}

		public void StartList()
		{
			CheckNotEndOfDocument ();
			if (DataType == DataTypes.List) {
				reader.ReadUInt8 ();
				state.Push (State.ArrayElement);
				ValueRead ();
			} else {
				throw new InvalidOperationException ("Attempted to start reading a list when list is not found.");
			}
		}

		public void EndDictionary()
		{
			CheckNotEndOfDocument ();
			if (state.Peek() == State.EndOfMap) {
				reader.ReadUInt8 ();
				state.Pop ();
				ValueRead ();
			} else {
				throw new InvalidOperationException ("Attempted to end reading a dictionary when the end of dictionary is not found.");
			}
		}
		public void EndList()
		{
			CheckNotEndOfDocument ();
			if (state.Peek() == State.EndOfArray) {
				reader.ReadUInt8 ();
				state.Pop ();
				ValueRead ();
			} else {
				throw new InvalidOperationException ("Attempted to end reading a list when the end of list is not found.");
			}
		}



		public struct String: IEquatable<string>
		{
			readonly byte[] buffer;
			readonly int offset;

			byte DataType
			{
				get { return buffer[offset]; }
			}

			internal String(byte[] buffer, int offset)
			{
				this.buffer = buffer;
				this.offset = offset;
			}

			static byte[] emptyBytes = new byte[0];

			public byte[] GetBytes()
			{
				var type = DataType;
				switch (type) {
				case DataTypes.StringifiedSignedInteger8:
				case DataTypes.StringifiedInteger8:
				case DataTypes.StringifiedInteger16:
				case DataTypes.StringifiedInteger32:
				case DataTypes.StringifiedInteger64:
					return utf8.GetBytes (ToString ());
				case DataTypes.String8:
				case DataTypes.String16:
				case DataTypes.String24:
				case DataTypes.String32:
					uint numBytes = 0;
					int offs = offset + 1;
					checked {
						switch (type) {
						case DataTypes.EmptyString:
							numBytes = 0;
							break;
						case DataTypes.String8:
							numBytes = buffer [offs++];
							numBytes += 1;
							break;
						case DataTypes.String16:
							numBytes = buffer[offs++];
							numBytes |= (uint)buffer[offs++] << 8;
							numBytes += 1 + 0x100;
							break;
						case DataTypes.String24:
							numBytes = buffer[offs++];
							numBytes |= (uint)buffer[offs++] << 8;
							numBytes |= (uint)buffer[offs++] << 16;
							numBytes += 1 + 0x10100;
							break;
						case DataTypes.String32:
							numBytes = buffer[offs++];
							numBytes |= (uint)buffer[offs++] << 8;
							numBytes |= (uint)buffer[offs++] << 16;
							numBytes |= (uint)buffer[offs++] << 24;
							numBytes += 1 + 0x1010100;
							break;
						}
					}
					if (numBytes > (uint)int.MaxValue) {
						throw new OverflowException ();
					}
					byte[] b = new byte[(int)numBytes];
					Buffer.BlockCopy (buffer, offs, b, 0, (int)numBytes);
					return b;
				case DataTypes.EmptyString:
					return emptyBytes;
				default:
					throw new InvalidOperationException ();
				}
			}

			public override string ToString ()
			{
				var type = DataType;
				switch (DataType) {
				case DataTypes.StringifiedSignedInteger8:
					int v = (sbyte)buffer [offset + 1];
					return v.ToString ();
				case DataTypes.StringifiedInteger8:
				case DataTypes.StringifiedInteger16:
				case DataTypes.StringifiedInteger32:
				case DataTypes.StringifiedInteger64:
					ulong val = 0;
					checked {
						switch (type) {
						case DataTypes.StringifiedInteger8:
							val = buffer [offset + 1];
							break;
						case DataTypes.StringifiedInteger16:
							val = buffer [offset + 1];
							val |= (ulong)buffer [offset + 2] << 8;
							val += 0x100;
							break;
						case DataTypes.StringifiedInteger32:
							val = buffer [offset + 1];
							val |= (ulong)buffer [offset + 2] << 8;
							val |= (ulong)buffer [offset + 3] << 16;
							val |= (ulong)buffer [offset + 4] << 24;
							val += 0x10100;
							break;
						case DataTypes.StringifiedInteger64:
							val = buffer [offset + 1];
							val |= (ulong)buffer [offset + 2] << 8;
							val |= (ulong)buffer [offset + 3] << 16;
							val |= (ulong)buffer [offset + 4] << 24;
							val |= (ulong)buffer [offset + 5] << 32;
							val |= (ulong)buffer [offset + 6] << 40;
							val |= (ulong)buffer [offset + 7] << 48;
							val |= (ulong)buffer [offset + 8] << 56;
							val += 0x100010100;
							break;
						}
						val += 128;
					}
					return val.ToString ();
				case DataTypes.EmptyString:
					return string.Empty;
				case DataTypes.String8:
				case DataTypes.String16:
				case DataTypes.String24:
				case DataTypes.String32:
					return utf8.GetString (GetBytes ());
				default:
					throw new InvalidOperationException ();
				}
			}

			public bool Equals (string str)
			{
				var type = DataType;
				switch (type) {
				case DataTypes.EmptyString:
					return str.Length == 0;
				case DataTypes.StringifiedSignedInteger8:
				case DataTypes.StringifiedInteger8:
				case DataTypes.StringifiedInteger16:
				case DataTypes.StringifiedInteger32:
				case DataTypes.StringifiedInteger64:
					if (str.Length > 0 && StonWriter.IsPossiblyStringifiedInteger (str)) {
						switch (type) {
						case DataTypes.StringifiedSignedInteger8:
							int v = buffer [offset + 1];
							{
								long l;
								if (long.TryParse (str, out l)) {
									return v == l;
								} else {
									return ToString ().Equals (str);
								}
							}
						case DataTypes.StringifiedInteger8:
						case DataTypes.StringifiedInteger16:
						case DataTypes.StringifiedInteger32:
						case DataTypes.StringifiedInteger64:
							ulong val = 0;
							checked {
								switch (type) {
								case DataTypes.StringifiedInteger8:
									val = buffer [offset + 1];
									break;
								case DataTypes.StringifiedInteger16:
									val = buffer [offset + 1];
									val |= (ulong)buffer [offset + 2] << 8;
									val += 0x100;
									break;
								case DataTypes.StringifiedInteger32:
									val = buffer [offset + 1];
									val |= (ulong)buffer [offset + 2] << 8;
									val |= (ulong)buffer [offset + 3] << 16;
									val |= (ulong)buffer [offset + 4] << 24;
									val += 0x10100;
									break;
								case DataTypes.StringifiedInteger64:
									val = buffer [offset + 1];
									val |= (ulong)buffer [offset + 2] << 8;
									val |= (ulong)buffer [offset + 3] << 16;
									val |= (ulong)buffer [offset + 4] << 24;
									val |= (ulong)buffer [offset + 5] << 32;
									val |= (ulong)buffer [offset + 6] << 40;
									val |= (ulong)buffer [offset + 7] << 48;
									val |= (ulong)buffer [offset + 8] << 56;
									val += 0x100010100;
									break;
								}
								val += 128;
							}
							if (str [0] == '-')
								return false;
							{
								ulong l;
								if (ulong.TryParse (str, out l)) {
									return val == l;
								} else {
									return ToString ().Equals (str);
								}
							}
						default:
							throw new InvalidOperationException ();
						}
					} else {
						return false;
					}
				}
				return ToString ().Equals (str);
			}

			public static implicit operator string (String s)
			{
				return s.ToString();
			}
			public static implicit operator byte[] (String s)
			{
				return s.GetBytes ();
			}
		}
	}
}


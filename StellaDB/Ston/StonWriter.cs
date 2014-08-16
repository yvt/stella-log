using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.Ston
{
	public sealed class StonWriter
	{
		enum State
		{
			Scalar,
			MapKey,
			MapValue,
			ArrayElement
		}

		readonly Utils.MemoryBinaryWriter bw;
		readonly Stack<State> state = new Stack<State>();

		public StonWriter ()
		{
			bw = new Utils.MemoryBinaryWriter ();
			state.Push (State.Scalar);
		}

		internal StonWriter(Utils.MemoryBinaryWriter bw)
		{
			if (bw == null)
				throw new ArgumentNullException ("bw");

			this.bw = bw;
			state.Push (State.Scalar);
		}

		public void Reset()
		{
			bw.Length = 0;
			bw.Position = 0;
			state.Clear ();
			state.Push (State.Scalar);
		}

		public byte[] ToArray()
		{
			CheckComplete ();
			return bw.ToArray ();
		}

		public struct Buffer
		{
			public byte[] Bytes;
			public int Length;
		}
		public Buffer GetBuffer()
		{
			CheckComplete ();
			return new Buffer {
				Bytes = bw.GetBuffer(),
				Length = bw.Length
			};
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		void CheckComplete()
		{
			if (state.Count > 0) {
				throw new InvalidOperationException ("STON document is not complete.");
			}
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		State CheckState()
		{
			if (state.Count == 0) {
				throw new InvalidOperationException ("Cannot emit multiple root objects.");
			}
			return state.Peek ();
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		void CheckStateForNonStringValue(string type)
		{
			if (CheckState() == State.MapKey) {
				ThrowNotAllowed (type);
			}
		}
			
		void ThrowNotAllowed(string type)
		{
			switch (CheckState()) {
			case State.Scalar:
				throw new InvalidOperationException 
				(string.Format("Cannot write {0} as the root object.", type));
			case State.MapKey:
				throw new InvalidOperationException 
				(string.Format("Cannot write {0} as the dictionary key.", type));
			case State.MapValue:
				throw new InvalidOperationException 
				(string.Format("Cannot write {0} as the dictionary value.", type));
			case State.ArrayElement:
				throw new InvalidOperationException 
				(string.Format("Cannot write {0} as the array element.", type));
			default:
				throw new InvalidOperationException ();
			}
		}

		void ValueWritten()
		{
			switch (state.Pop ()) {
			case State.Scalar:
				break;
			case State.MapKey:
				state.Push (State.MapValue);
				break;
			case State.MapValue:
				state.Push (State.MapKey);
				break;
			case State.ArrayElement:
				state.Push (State.ArrayElement);
				break;
			default:
				throw new InvalidOperationException ();
			}
		}

		public void WriteNull()
		{
			CheckStateForNonStringValue ("the null value");
			bw.Write (DataTypes.Null);
			ValueWritten ();
		}
			
		public void Write(byte[] utf8)
		{
			CheckState ();

			if (utf8.Length == 0) {
				bw.Write (DataTypes.EmptyString);
			} else if (utf8.Length <= 0x100) {
				bw.Write ((byte)DataTypes.String8);
				bw.Write ((byte)(utf8.Length - 1));
			} else if (utf8.Length <= 0x10100) {
				bw.Write ((byte)DataTypes.String16);
				int l = utf8.Length - 0x101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
			} else if (utf8.Length <= 0x1010100) {
				bw.Write ((byte)DataTypes.String24);
				int l = utf8.Length - 0x10101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
				bw.Write ((byte)(l >> 16));
			} else {
				bw.Write ((byte)DataTypes.String32);
				int l = utf8.Length - 0x1010101;
				bw.Write ((byte)(l));
				bw.Write ((byte)(l >> 8));
				bw.Write ((byte)(l >> 16));
				bw.Write ((byte)(l >> 24));
			}
			bw.Write (utf8);
			ValueWritten ();
		}

		static internal bool IsPossiblyStringifiedInteger(string s)
		{
			bool hasNonZero = false;
			if (s == "0") {
				return true;
			}
			for(int i = 0, count = s.Length; i < count; ++i) {
				var c = s [i];
				if (c >= '0' && c <= '9') {
					if (c == '0') {
						if (!hasNonZero) {
							// Preceding zero disables the optimization
							return false;
						}
					} else {
						hasNonZero = true;
					}
					continue;
				} else if (c == '-' && i == 0) {
					continue;
				} else {
					return false;
				}
			}
			if (!hasNonZero) {
				// something like "-"
				return false;
			}
			return true;
		}

		static readonly System.Text.Encoding utf8 = new System.Text.UTF8Encoding ();

		public void Write(string str)
		{
			CheckState ();

			// See if this can be encoded as stringified integer
			if (str.Length > 0 && IsPossiblyStringifiedInteger(str)) {
				ulong lval;
				if (str[0] != '-' && ulong.TryParse(str, out lval) && lval.ToString().Equals(str)) {
					if (lval < 128) {
						bw.Write (DataTypes.StringifiedSignedInteger8);
						bw.Write ((byte)(lval));
						ValueWritten ();
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
					ValueWritten ();
					return;
				}

				sbyte sval;
				if (sbyte.TryParse(str, out sval) && sval.ToString().Equals(str)) {
					bw.Write (DataTypes.StringifiedSignedInteger8);
					bw.Write ((byte)(sval));
					ValueWritten ();
					return;
				}
			}

			Write(utf8.GetBytes(str));
		}

		public void Write(long v)
		{
			CheckStateForNonStringValue ("an integer value");

			if (v == 0) {
				bw.Write (DataTypes.ZeroInt);
				ValueWritten ();
				return;
			} else if (v >= -64 && v < 0) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)v & 0x7f)));
				ValueWritten ();
				return;
			} else if (v > 0 && v <= 64) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)(v - 1) & 0x7f)));
				ValueWritten ();
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
			ValueWritten ();
		}

		public void Write(StonInteger v)
		{
			long signedval;
			if (v.TryConvertToInt64(out signedval)) {
				Write (signedval);
			} else {
				Write (v.ToUInt64 ());
			}
		}

		public void Write(ulong v)
		{
			CheckStateForNonStringValue ("an integer value");

			if (v == 0) {
				bw.Write (DataTypes.ZeroInt);
				ValueWritten ();
				return;
			} else if (v > 0 && v <= 64) {
				bw.Write ((byte)(DataTypes.IntegerBase | ((int)(v - 1) & 0x7f)));
				ValueWritten ();
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
			ValueWritten ();
		}

		public void Write(char c)
		{
			CheckStateForNonStringValue ("a character value");

			var utf16 = (ushort)c;
			if (utf16 < 0x100) {
				bw.Write (DataTypes.Char8);
				bw.Write ((byte)utf16);
			} else {
				bw.Write (DataTypes.Char16);
				bw.Write ((byte)utf16);
				bw.Write ((byte)(utf16 >> 8));
			}
			ValueWritten ();
		}

		public void Write(bool v)
		{
			CheckStateForNonStringValue ("a boolean value");

			bw.Write (v ? DataTypes.True : DataTypes.False);
			ValueWritten ();
		}

		public void Write(float f)
		{
			CheckStateForNonStringValue ("a single-precision floating point value");
			throw new NotImplementedException ("Serializing float is not implemented yet.");
		}

		public void Write(double f)
		{
			CheckStateForNonStringValue ("a double-precision floating point value");
			bw.Write (DataTypes.Double);
			bw.Write (BitConverter.DoubleToInt64Bits (f));
			ValueWritten ();
		}

		public void StartList()
		{
			CheckStateForNonStringValue ("a list");
			bw.Write (DataTypes.List);
			state.Push (State.ArrayElement);
		}

		public void EndList()
		{
			if (CheckState() != State.ArrayElement) {
				throw new InvalidOperationException ("Attempted to close a list while not writing a list.");
			}

			bw.Write (DataTypes.EOMLMarker);
			state.Pop ();
			ValueWritten ();
		}

		public void StartDictionary()
		{
			CheckStateForNonStringValue ("a dictionary");
			bw.Write (DataTypes.Map);
			state.Push (State.MapKey);
		}

		public void EndDictionary()
		{
			if (CheckState() == State.MapValue) {
				throw new InvalidOperationException ("Attempted to close a dictionary while the key/value pair being written is incomplete.");
			}
			if (CheckState() != State.MapKey) {
				throw new InvalidOperationException ("Attempted to close a dictionary while not writing a dictionary.");
			}

			bw.Write (DataTypes.EOMLMarker);
			state.Pop ();
			ValueWritten ();
		}
	}
}


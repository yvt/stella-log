using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Yavit.StellaDB.Ston
{
	public class StonSerializer
	{

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
			
		#region Type Converter
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

					StonConverter converter = GetConverter(type);
					if (converter != null) {
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
		#endregion

		#region Deserialization
		object DeserializeImpl(StonReader reader, int level = 0)
		{
			switch (reader.CurrentNodeType) {
			case StonReader.NodeType.EndOfDocument:
				throw new StonException ("Unexpected end of document.");
			case StonReader.NodeType.Null:
				reader.ReadNull ();
				return null;
			case StonReader.NodeType.Dictionary:
				reader.StartDictionary ();
				var dic = new Dictionary<string, object> ();
				while (reader.CurrentNodeType != StonReader.NodeType.EndOfCollection) {
					var key = reader.CurrentDictionaryKey.ToString ();
					dic.Add (key, DeserializeImpl (reader, level + 1));
				}
				reader.EndDictionary ();
				return dic;
			case StonReader.NodeType.List:
				reader.StartList ();
				var list = new List<object> ();
				while (reader.CurrentNodeType != StonReader.NodeType.EndOfCollection) {
					list.Add (DeserializeImpl (reader, level + 1));
				}
				reader.EndList ();
				return list;
			case StonReader.NodeType.EndOfCollection:
				throw new StonException ("Unexpected end of collection.");
			case StonReader.NodeType.Integer:
				return reader.ReadInteger ().ToObject();
			case StonReader.NodeType.Float:
				return reader.ReadFloat ();
			case StonReader.NodeType.Double:
				return reader.ReadDouble ();
			case StonReader.NodeType.Boolean:
				return reader.ReadBoolean ();
			case StonReader.NodeType.Char:
				return reader.ReadChar ();
			case StonReader.NodeType.String:
				return reader.ReadString ().ToString ();
			case StonReader.NodeType.ByteArray:
				return reader.ReadString ().GetBytes();
			default:
				throw new InvalidOperationException ();
			}
		}

		internal object DeserializeObject(StonReader reader)
		{
			if (reader == null)
				throw new ArgumentNullException ("reader");
			return DeserializeImpl (reader);
		}
		public object DeserializeObject(byte[] buffer)
		{
			if (buffer == null)
				throw new ArgumentNullException ("buffer");
			return DeserializeObject (new StonReader(buffer));
		}

		public object Deserialize(byte[] buffer, Type type)
		{
			return ConvertToType (DeserializeObject (buffer), type);
		}

		public T Deserialize<T>(byte[] buffer)
		{
			return (T)Deserialize (buffer, typeof(T));
		}
		#endregion
			
		#region Serialization
		void SerializeImpl(object obj, StonWriter writer, int level)
		{
			if (level > RecursionLimit) {
				throw new InvalidOperationException ("Recursion limit reached.");
			}

			if (obj == null) {
				writer.WriteNull ();
			} else {
				switch (Convert.GetTypeCode (obj)) {
				case TypeCode.Empty:
				case TypeCode.DBNull:
					writer.WriteNull ();
					break;
				case TypeCode.Boolean:
					writer.Write (Convert.ToBoolean (obj));
					break;
				case TypeCode.Char:
					writer.Write (Convert.ToChar (obj));
					break;
				case TypeCode.String:
					writer.Write (Convert.ToString (obj));
					break;
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
					writer.Write (Convert.ToInt64 (obj));
					break;
				case TypeCode.UInt64:
					writer.Write (Convert.ToUInt64 (obj));
					break;
				case TypeCode.Single:
					writer.Write (Convert.ToSingle (obj));
					break;
				case TypeCode.Double:
					writer.Write (Convert.ToDouble (obj));
					break;
				case TypeCode.Decimal:
					throw new InvalidOperationException (
						string.Format("Unserializable value {0} which is Decimal.",
							obj));
				case TypeCode.DateTime:
					throw new NotImplementedException ("Serializing DateTime is not implemented yet.");
				case TypeCode.Object:
					var bytes = obj as byte[];
					if (bytes != null) {
						writer.Write(bytes);
						break;
					}
					var dic = obj as IEnumerable<KeyValuePair<string, object>>;
					var enumerable = obj as IEnumerable<object>;
					if (enumerable != null) {
						writer.StartList ();
						foreach (var e in enumerable) {
							SerializeImpl (e, writer, level + 1);
						}
						writer.EndList ();
						break;
					} 
					if (dic == null) {
						StonConverter cvt = GetConverter(obj.GetType());
						if (cvt != null) {
							dic = cvt.Serialize (obj, this);
						} else {
							// might be arbitary IDictionary<string, T> where T != object.
							// note that IDictionary<string, T> is not compatible with
							// IDictionary<string, object>.
							ICovariantDictionaryWrapper wrapper;
							if (covarianceWrapper.TryGetValue (obj.GetType (), out wrapper)) {
								dic = wrapper.Convert (obj);
							} else {
								foreach (var i in obj.GetType().GetInterfaces()) {
									if (i.IsGenericType &&
									    i.GetGenericTypeDefinition () == typeof(IDictionary<,>)) {
										var param = i.GetGenericArguments ();
										if (param [0] == typeof(string)) {
											var wrapperType = typeof(CovariantDictionaryWrapper<>);
											wrapperType = wrapperType.MakeGenericType (param [1]);
											wrapper = (ICovariantDictionaryWrapper)
												wrapperType.GetConstructor (new Type[]{ }).Invoke (null);
											covarianceWrapper.Add (obj.GetType (), wrapper);
											dic = wrapper.Convert (obj);
											break;
										}
									}
								}
							}
						}
					}

					writer.StartDictionary ();
					foreach (var e in dic) {
						SerializeImpl (e.Key, writer, level + 1);
						SerializeImpl (e.Value, writer, level + 1);
					}
					writer.EndDictionary ();
					break;
				default:
					throw new InvalidOperationException ();
				}
			}
		}

		internal void Serialize(object obj, StonWriter writer)
		{
			SerializeImpl (obj, writer, 0);
		}

		public byte[] Serialize(object obj)
		{
			var sw = new StonWriter ();
			Serialize (obj, sw);
			return sw.ToArray ();
		}
		#endregion

		StonConverter GetConverter(Type type)
		{
			StonConverter converter;
			if (converters.TryGetValue(type, out converter)) {
				return converter;
			}

			// Class with SerializableAttribute can be serialized
			if (type.GetCustomAttributes(typeof(SerializableAttribute), false).Length > 0) {
				// But, don't treat IDictionary<string, T> as Serializable.
				foreach (var i in type.GetInterfaces()) {
					if (i.IsGenericType &&
						i.GetGenericTypeDefinition () == typeof(IDictionary<,>)) {
						var param = i.GetGenericArguments ();
						if (param [0] == typeof(string)) {
							return null;
						}
					}
				}

				converter = new StonConverterForSerializable (type);
				converters.Add (type, converter);
				return converter;
			}

			return null;
		}

		#region CovarianceWrapper
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
		#endregion
	}
}


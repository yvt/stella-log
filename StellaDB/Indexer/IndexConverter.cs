using System;
using Yavit.StellaDB.Ston;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB.Indexer
{
	sealed class IndexConverter: StonConverter
	{

		Type GetTypeForFieldName(string name)
		{
			switch (name) {
			case "num":
				return typeof(NumericKeyParameters);
			case "bin":
				return typeof(BinaryKeyParameters);
			}
			throw new ArgumentException (string.Format("Invalid field type {0}.", name),"name");
		}

		public override object Deserialize (IDictionary<string, object> dictionary, Type type, StonSerializer serializer)
		{
			if (type == typeof(IndexParameters)) {
				var fields = dictionary ["fields"] as IEnumerable<object>;
				return new IndexParameters () {
					Fields = from obj in fields
					         select () => {
						var dic = (IDictionary<string, object>)obj;
						return serializer.ConvertToType (dic ["param"], GetTypeForFieldName ((string)dic ["type"]));
					}
				};
			} else if (type == typeof(NumericKeyParameters)) {
				return new NumericKeyParameters ();
			} else if (type == typeof(BinaryKeyParameters)) {
				return new BinaryKeyParameters () {
					KeyLength = serializer.ConvertToType<int>(dictionary["len"])
				};
			} else {
				throw new ArgumentException ("type");
			}
		}

		string GetFieldTypeName(object param)
		{
			if (param is NumericKeyParameters) return "num";
			else if (param is BinaryKeyParameters) return "bin";
			throw new ArgumentException ("param");
		}

		public override IDictionary<string, object> Serialize (object obj, StonSerializer serializer)
		{
			if (obj is IndexParameters) {
				var param = (IndexParameters)obj;
				return new Dictionary<string, object> {
					{ "fields", from field in param.Fields
						select new Dictionary<string, object> {
							{ "type", GetFieldTypeName(field) },
							{ "param", field }
						}}
				};
			} else if (obj is NumericKeyParameters) {
				var param = (NumericKeyParameters)obj;
				return new Dictionary<string, object> ();
			} else if (obj is BinaryKeyParameters) {
				var param = (BinaryKeyParameters)obj;
				return new Dictionary<string, object> {
					{ "len", param.KeyLength }
				};
			} else {
				throw new ArgumentException ("obj");
			}
		}

		static readonly IEnumerable<Type> types = new Type[] {
			typeof(NumericKeyParameters),
			typeof(BinaryKeyParameters),
			typeof(IndexParameters)
		};

		public override IEnumerable<Type> SupportedTypes {
			get {
				return types;
			}
		}

	}
}


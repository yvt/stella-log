using System;
using System.Collections.Generic;

namespace Yavit.StellaDB.Ston
{
	public abstract class StonConverter
	{
		public abstract IEnumerable<Type> SupportedTypes { get; }
		public abstract object Deserialize 
		(IDictionary<string, object> dictionary, Type type, StonSerializer serializer);
		public abstract IDictionary<string, object> Serialize
		(object obj, StonSerializer serializer);
	}
}


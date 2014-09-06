using System;
using System.Collections.Generic;
using System.Reflection;
using System.ComponentModel;
using System.Linq;
using Exprs = System.Linq.Expressions;

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

	#region Generated StonConverter

	/// <summary>
	/// Ston converter for classes with SerializableAttribute.
	/// </summary>
	sealed class StonConverterForSerializable: StonConverter
	{
		readonly Type type;
		readonly Type[] types;
		readonly Field[] fields;

		int usageCount = 0;
		Func<IDictionary<string, object>, object> deserializer;
		Func<object, IDictionary<string, object>> serializer;

		class Field
		{
			public FieldInfo Info;
			public string Name;
			public DefaultValueAttribute DefaultValue;
		}

		public StonConverterForSerializable(Type type)
		{
			this.type = type;
			types = new [] { type };

			fields = type.GetFields ().Select(HandleField).Where(f => f != null).ToArray();
		}

		Field HandleField(FieldInfo field)
		{
			if (field.GetCustomAttributes(typeof(NonSerializedAttribute), false).Length > 0) {
				return null;
			}

			return new Field () {
				Info = field,
				Name = field.Name,
				DefaultValue = (DefaultValueAttribute)field
					.GetCustomAttributes(typeof(DefaultValueAttribute), false)
					.FirstOrDefault()
			};
		}

		static readonly Type dicType = typeof(IDictionary<string, object>);
		static readonly MethodInfo dicTryGetValue = dicType.GetMethod ("TryGetValue", new [] {
			typeof(string), typeof(object).MakeByRefType()
		});
		static readonly MethodInfo dicAdd = dicType.GetMethod ("Add", new [] {
			typeof(string), typeof(object)
		});
		static readonly Type serializerType = typeof(StonSerializer);
		static readonly MethodInfo serializerConvertToType =
			serializerType.GetMethod ("ConvertToType", new [] {typeof(object)});
		static readonly Type objectType = typeof(object);
		static readonly MethodInfo objectEquals = 
			objectType.GetMethod("Equals", new [] {typeof(object), typeof(object)});

		public override object Deserialize (IDictionary<string, object> dictionary, Type type, StonSerializer serializer)
		{
			if (dictionary == null) {
				return null;
			}

			++usageCount;
			if (usageCount >= 3 && deserializer == null) {
				// Create optimized serializer
				var param = Exprs.Expression.Parameter (dicType, "dictionary");
				var tmpObj = Exprs.Expression.Variable (typeof(object), "dictionaryValue");
				var returnedObj = Exprs.Expression.Variable (this.type, "returnedObject");
				var serExpr = Exprs.Expression.Constant(serializer);

				var exprs = new Exprs.Expression[] {
					Exprs.Expression.Assign(returnedObj, Exprs.Expression.New (this.type))
				}.Concat (fields.Select (field => {
					var nameExpr = Exprs.Expression.Constant(field.Name);
					var cvtType = serializerConvertToType.MakeGenericMethod(new [] { field.Info.FieldType });
					var convertExpr = Exprs.Expression.Call(serExpr, cvtType, tmpObj);
					var assignExpr = Exprs.Expression.Assign(Exprs.Expression.Field(returnedObj, field.Info),
						convertExpr);

					return Exprs.Expression.IfThen(
						Exprs.Expression.Call(param, dicTryGetValue, nameExpr, tmpObj),
						assignExpr);
				})).Concat(new [] { returnedObj });

				var expr = Exprs.Expression.Block (new[] {
					tmpObj, returnedObj
				}, exprs);

				var func = Exprs.Expression.Lambda<Func<IDictionary<string, object>, object>> (expr,
					           new [] { param });
				deserializer = func.Compile ();
			}
			if (deserializer != null) {
				return deserializer (dictionary);
			}

			// Slow path
			var ret = type.GetConstructor (new Type[]{ }).Invoke (new object[]{ });
			foreach (var field in fields) {
				object v;
				if (dictionary.TryGetValue(field.Name, out v)) {
					object converted = serializer.ConvertToType (v, field.Info.FieldType);
					field.Info.SetValue (ret, converted);
				}
			}
			return ret;
		}

		public override IDictionary<string, object> Serialize (object obj, StonSerializer sserializer)
		{
			if (obj == null) {
				return null;
			}

			++usageCount;
			if (usageCount >= 3 && serializer == null) {
				// Create optimized serializer.
				var param = Exprs.Expression.Parameter (typeof(object), "inputObject");
				var typedParam = Exprs.Expression.Variable (type, "inputTyped");
				var dic = Exprs.Expression.Variable (dicType, "dictionary");
				var expr = Exprs.Expression.Block (
					new [] {
						typedParam, dic
					},
					new Exprs.Expression [] {
						Exprs.Expression.Assign(typedParam,	
							Exprs.Expression.Convert(param, type)),
						Exprs.Expression.Assign(dic,
							Exprs.Expression.New(typeof(Dictionary<string, object>)))
					}.Concat (fields.Select (field => {
						var fieldexpr = Exprs.Expression.Convert(Exprs.Expression.Field (typedParam, field.Info),
							typeof(object));
						var addExpr = Exprs.Expression.Call (dic, dicAdd, 
							              Exprs.Expression.Constant (field.Name),
							              fieldexpr);
						if (field.DefaultValue != null) {
							return (Exprs.Expression)Exprs.Expression.IfThen (
								Exprs.Expression.IsFalse (
									Exprs.Expression.Call (objectEquals, fieldexpr,
										Exprs.Expression.Constant (field.DefaultValue.Value, typeof(object)))
								),
								addExpr
							);
						}
						return (Exprs.Expression)addExpr;
					})).Concat(new Exprs.Expression[] {
						dic
					}));

				var serExpr = Exprs.Expression.Lambda<Func<object, IDictionary<string, object>>>(
					expr, new [] {param});

				serializer = serExpr.Compile ();
			}
			if (serializer != null) {
				return serializer (obj);
			}

			var ret = new Dictionary<string, object> ();
			foreach (var field in fields) {
				var val = field.Info.GetValue (obj);
				if (field.DefaultValue != null) {
					if (object.Equals(val, field.DefaultValue.Value)) {
						continue;
					}
				}
				ret.Add (field.Name, val);
			}
			return ret;
		}

		public override IEnumerable<Type> SupportedTypes {
			get {
				return types;
			}
		}
	}

	#endregion
}


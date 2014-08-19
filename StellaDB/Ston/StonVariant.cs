using System;
using System.Collections.Generic;

namespace Yavit.StellaDB.Ston
{
	/// <summary>
	/// Class that represents a value of STON. Used for easier query condition construction.
	/// </summary>
	public abstract class StonVariant
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
		object obj;
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
}


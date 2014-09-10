using System;
using System.Collections.Generic;

namespace Yavit.StellaLog.Core.Utils
{
	internal sealed class WeakValueDictionary<TKey, TValue>: 
	IDictionary<TKey, TValue>, IEnumerable<KeyValuePair<TKey, TValue>>
		where TValue : class
	{
		// .NET 4.0 doesn't have WeakReference<T>
		readonly Dictionary<TKey, WeakReference> dic;
		public WeakValueDictionary ()
		{
			dic = new Dictionary<TKey, WeakReference> ();
		}
		public WeakValueDictionary (IEqualityComparer<TKey> comparer)
		{
			dic = new Dictionary<TKey, WeakReference> (comparer);
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			WeakReference r;
			if (dic.TryGetValue(key, out r)) {
				value = (TValue) r.Target;
				var isAlive = r.IsAlive;
				if (!isAlive) {
					dic.Remove(key);
				}
				return isAlive;
			} else {
				value = default(TValue);
			}
			return false;
		}

		public void Add (TKey key, TValue value)
		{
			if (value == null) {
				throw new ArgumentNullException ("value");
			}
			dic.Add (key, new WeakReference (value));
		}
		public bool ContainsKey (TKey key)
		{
			WeakReference r;
			if (dic.TryGetValue(key, out r)) {
				return r.IsAlive;
			}
			return false;
		}
		public bool Remove (TKey key)
		{
			return dic.Remove (key);
		}
		public TValue this [TKey index] {
			get {
				TValue r;
				if (!TryGetValue(index, out r)) {
					throw new KeyNotFoundException ();
				}
				return r;
			}
			set {
				Add (index, value);
			}
		}

		sealed class KeyCollection: ICollection<TKey>
		{
			readonly WeakValueDictionary<TKey, TValue> d;
			public KeyCollection(WeakValueDictionary<TKey, TValue> d)
			{ this.d = d; }
			public void Add (TKey item)
			{ throw new NotSupportedException (); }
			public void Clear ()
			{ throw new NotSupportedException (); }
			public bool Contains (TKey item)
			{
				return d.ContainsKey (item);
			}
			public void CopyTo (TKey[] array, int arrayIndex)
			{
				foreach (var e in this)
				{
					array [arrayIndex++] = e;
				}
			}
			public bool Remove (TKey item)
			{ throw new NotSupportedException (); }
			public int Count {
				get { return d.Count; }
			}
			public bool IsReadOnly {
				get { return true; }
			}
			public IEnumerator<TKey> GetEnumerator ()
			{
				foreach (var e in d)
					yield return e.Key;
			}
			System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
			{
				foreach (TKey e in this)
					yield return e;
			}
		}
		public ICollection<TKey> Keys {
			get { return new KeyCollection (this); }
		}

		sealed class ValueCollection: ICollection<TValue>
		{
			readonly WeakValueDictionary<TKey, TValue> d;
			public ValueCollection(WeakValueDictionary<TKey, TValue> d)
			{ this.d = d; }
			public void Add (TValue item)
			{ throw new NotSupportedException (); }
			public void Clear ()
			{ throw new NotSupportedException (); }
			public bool Contains (TValue item)
			{
				IEqualityComparer<TValue> d = EqualityComparer<TValue>.Default;
				foreach (var e in this)
				{
					if (d.Equals(e, item)) {
						return true;
					}
				}
				return false;
			}
			public void CopyTo (TValue[] array, int arrayIndex)
			{
				foreach (var e in this)
				{
					array [arrayIndex++] = e;
				}
			}
			public bool Remove (TValue item)
			{ throw new NotSupportedException (); }
			public int Count {
				get { return d.Count; }
			}
			public bool IsReadOnly {
				get { return true; }
			}
			public IEnumerator<TValue> GetEnumerator ()
			{
				foreach (var e in d)
					yield return e.Value;
			}
			System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
			{
				foreach (TValue e in this)
					yield return e;
			}
		}
		public ICollection<TValue> Values {
			get {
				return new ValueCollection (this);
			}
		}
		public void Add (KeyValuePair<TKey, TValue> item)
		{
			Add (item.Key, item.Value);
		}
		public void Clear ()
		{
			dic.Clear ();
		}
		public bool Contains (KeyValuePair<TKey, TValue> item)
		{
			TValue v;
			if (TryGetValue(item.Key, out v)) {
				IEqualityComparer<TValue> d = EqualityComparer<TValue>.Default;
				return d.Equals (v, item.Value);
			}
			return false;
		}
		public void CopyTo (KeyValuePair<TKey, TValue>[] array, int arrayIndex)
		{
			foreach (var e in this)
			{
				array [arrayIndex++] = e;
			}
		}
		public bool Remove (KeyValuePair<TKey, TValue> item)
		{
			TValue v;
			if (TryGetValue(item.Key, out v)) {
				IEqualityComparer<TValue> d = EqualityComparer<TValue>.Default;
				if(d.Equals (v, item.Value)) {
					Remove (item.Key);
					return true;
				}
			}
			return false;
		}
		public int Count {
			get {
				int count = 0;
				foreach (KeyValuePair<TKey, TValue> e in this)
					++count;
				return count;
			}
		}
		public bool IsReadOnly {
			get {
				return false;
			}
		}
		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator ()
		{
			var wasted = new List<TKey> ();
			foreach (var e in dic) {
				var v = e.Value.Target;
				if (e.Value.IsAlive) {
					yield return new KeyValuePair<TKey, TValue> (e.Key, (TValue)v);
				} else {
					wasted.Add (e.Key);
				}
			}
			foreach(var k in wasted) {
				dic.Remove (k);
			}
		}
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
		{
			foreach (KeyValuePair<TKey, TValue> e in this)
				yield return e;
		}
	}
}


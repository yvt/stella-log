using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaLog.Core
{
	public sealed class Record: IDictionary<string, object>
	{
		readonly RecordManager manager;

		object[] attributes;

		long? recordId = null;
		internal bool needsReload = false;

		internal Record(RecordManager manager)
		{
			if (manager == null)
				throw new ArgumentNullException ("manager");
			this.manager = manager;
			this.attributes = new object[manager.AttributeCount];
		}

		~Record()
		{
			if (recordId != null) {
				manager.recordCache.Remove ((long)recordId);
			}
		}

		internal void MoveValuesFrom(Record r)
		{
			attributes = r.attributes;
		}

		void ReloadIfNeeded()
		{
			if (recordId != null && needsReload) {
				var ret = manager.table.Fetch ((long)recordId);
				if (ret == null) {
					throw new InvalidOperationException ("Record was deleted.");
				}
				MoveValuesFrom (ret.ToObject<Record> ());
				needsReload = false;
			}
		}

		public long? RecordId
		{
			get { return recordId; }
			internal set { 
				if (recordId != null) {
					manager.recordCache.Remove ((long)recordId);
				}
				recordId = value;
				if (recordId != null) {
					manager.recordCache.Add ((long)recordId, this);
				}
			}
		}

		static readonly DateTime dateEpoch = new DateTime(1970, 1, 1);
		static readonly Random r = new Random ();
		public static long GenerateRecordId()
		{
			return (long)(((ulong)(DateTime.UtcNow - dateEpoch).TotalMilliseconds << 16) | (ulong)(r.Next () & 0xffff));
		}

		public void Save()
		{
			ReloadIfNeeded ();
			if (recordId == null) {
				try {
					recordId = GenerateRecordId ();
					manager.currentUpdatingRecordId = recordId;
					manager.table.Update ((long)recordId, this);
				} catch {
					recordId = null;
					throw;
				} finally {
					manager.currentUpdatingRecordId = null;
				}
			} else {
				try {
					manager.currentUpdatingRecordId = recordId;
					manager.table.Update ((long)recordId, this);
				} finally {
					manager.currentUpdatingRecordId = null;
				}
			}
			needsReload = false;
		}

		public object this [string attributeName] 
		{
			get {
				ReloadIfNeeded ();
				var index = manager.GetAttributeIndex (attributeName);
				if (index.HasValue) {
					var i = index.Value;
					if (i < attributes.Length)
						return attributes [i];
				}
				return null;
			}
			set {
				ReloadIfNeeded ();
				var index = manager.EnsureAttributeIndex (attributeName);
				if (index >= attributes.Length) {
					if (value == null) {
						return;
					}
					Array.Resize (ref attributes, index + 1);
				}
				attributes [index] = value;
			}
		}

		public object this [int attributeIndex]
		{
			get {
				if (attributeIndex >= manager.AttributeCount ||
					attributeIndex < 0)
					throw new ArgumentOutOfRangeException ("attributeIndex");
				ReloadIfNeeded ();

				if (attributeIndex < attributes.Length)
					return attributes [attributeIndex];
				return null;
			}
			set {
				if (attributeIndex >= manager.AttributeCount ||
				    attributeIndex < 0)
					throw new ArgumentOutOfRangeException ("attributeIndex");
				ReloadIfNeeded ();

				if (attributeIndex >= attributes.Length) {
					if (value == null) {
						return;
					}
					Array.Resize (ref attributes, attributeIndex + 1);
				}
				attributes [attributeIndex] = value;
			}
		}

		public DateTime Time
		{
			get { return (DateTime)this[manager.timeAttributeIndex]; }
			set { this [manager.timeAttributeIndex] = value; }
		}

		public string Call
		{
			get { return (string)this[manager.callAttributeIndex]; }
			set { this [manager.callAttributeIndex] = value; }
		}

		public string Notes
		{
			get { return (string)this[manager.notesAttributeIndex]; }
			set { this [manager.notesAttributeIndex] = value; }
		}

		#region IDictionary
		void IDictionary<string, object>.Add (string key, object value)
		{
			this [key] = value;
		}

		bool IDictionary<string, object>.ContainsKey (string key)
		{
			return this [key] != null;
		}

		bool IDictionary<string, object>.Remove (string key)
		{
			var r = this [key] != null;
			this [key] = null;
			return r;
		}

		bool IDictionary<string, object>.TryGetValue (string key, out object value)
		{
			var ret = value = this [key];
			return ret != null;
		}

		ICollection<string> IDictionary<string, object>.Keys {
			get {
				ReloadIfNeeded ();
				return attributes.Select ((v, index) => new { Index = index, Value = v })
					.Where (e => e.Value != null)
					.Select (e => manager.GetAttributeName(e.Index)).ToArray();
			}
		}

		ICollection<object> IDictionary<string, object>.Values {
			get {
				ReloadIfNeeded ();
				return attributes.Select ((v, index) => new { Index = index, Value = v })
					.Where (e => e.Value != null)
					.Select (e => e.Value).ToArray();
			}
		}

		void ICollection<KeyValuePair<string, object>>.Add (KeyValuePair<string, object> item)
		{
			this [item.Key] = item.Value;
		}

		void ICollection<KeyValuePair<string, object>>.Clear ()
		{
			ReloadIfNeeded ();
			for (int i = 0; i < attributes.Length; ++i)
				attributes [i] = null;
		}

		bool ICollection<KeyValuePair<string, object>>.Contains (KeyValuePair<string, object> item)
		{
			return object.Equals (this [item.Key], item.Value);
		}

		void ICollection<KeyValuePair<string, object>>.CopyTo (KeyValuePair<string, object>[] array, int arrayIndex)
		{
			foreach (var e in (IDictionary<string, object>)this)
				array [arrayIndex++] = e;
		}

		bool ICollection<KeyValuePair<string, object>>.Remove (KeyValuePair<string, object> item)
		{
			if (((ICollection<KeyValuePair<string, object>>)this).Contains (item))
				return ((IDictionary<string, object>)this).Remove (item.Key);
			return false;
		}

		int ICollection<KeyValuePair<string, object>>.Count {
			get {
				ReloadIfNeeded ();
				return attributes.Count (e => e != null);
			}
		}

		bool ICollection<KeyValuePair<string, object>>.IsReadOnly {
			get {
				return false;
			}
		}

		IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator ()
		{
			ReloadIfNeeded ();
			for (int i = 0; i < attributes.Length; ++i)
				if (attributes [i] != null)
					yield return new KeyValuePair<string, object>(manager.GetAttributeName(i), attributes[i]);
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
		{
			foreach (var e in this)
				yield return e;
		}
		#endregion
	}
}


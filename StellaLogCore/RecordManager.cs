using System;
using System.Collections.Generic;

namespace Yavit.StellaLog.Core
{
	public sealed class RecordManager
	{
		readonly List<string> attributes = new List<string>();
		readonly Dictionary<string, int> attributeMap = new Dictionary<string, int>();

		internal readonly LogBook book;
		internal readonly VersionControlledTable table;

		readonly StellaDB.Ston.StonSerializer ston;

		internal readonly Utils.WeakValueDictionary<long, Record> recordCache =
			new Yavit.StellaLog.Core.Utils.WeakValueDictionary<long, Record>();

		internal long? currentUpdatingRecordId = null;

		internal readonly int timeAttributeIndex;
		internal readonly int callAttributeIndex;
		internal readonly int notesAttributeIndex;

		public RecordManager (LogBook book)
		{
			this.book = book;
			table = book.VersionController.GetTable ("Records");

			ston = new StellaDB.Ston.StonSerializer ();
			ston.RegisterConverters (new [] { new RecordConverter(this) });
			table.BaseTable.Serializer = ston;

			table.Updated += (sender, e) => {
				Record r;
				if (currentUpdatingRecordId == e.RowId) {
					return;
				}
				if (recordCache.TryGetValue(e.RowId, out r)) {
					r.needsReload = true;
					if (e.newValue.Length == 0) {
						// Deleted.
						r.RecordId = null;
					}
				}
			};

			timeAttributeIndex = EnsureAttributeIndex ("Time");
			callAttributeIndex = EnsureAttributeIndex ("Call");
			notesAttributeIndex = EnsureAttributeIndex ("Notes");

		}

		public int AttributeCount
		{
			get { return attributes.Count; }
		}

		public int? GetAttributeIndex(string name)
		{
			int index;
			if (attributeMap.TryGetValue(name, out index)) {
				return index;
			}
			return null;
		}

		public int EnsureAttributeIndex(string name)
		{
			int index;
			if (attributeMap.TryGetValue(name, out index)) {
				return index;
			} else {
				index = attributes.Count;
				attributes.Add (name);
				attributeMap.Add (name, index);
				return index;
			}
		}

		public string GetAttributeName(int index)
		{
			return attributes[index];
		}

		public Record Fetch(long recordId)
		{
			Record r;
			if (recordCache.TryGetValue(recordId, out r)) {
				return r;
			}

			// Read from the database
			var ret = table.Fetch (recordId);
			if (ret == null) {
				return null;
			}

			r = ret.ToObject<Record> ();
			r.RecordId = recordId;
			return r;
		}

		public Record CreateRecord()
		{
			return new Record(this);
		}
	}

	sealed class RecordConverter: StellaDB.Ston.StonConverter
	{
		readonly RecordManager manager;

		public RecordConverter(RecordManager manager)
		{
			this.manager = manager;

		}
		public override object Deserialize (IDictionary<string, object> dictionary, Type type, Yavit.StellaDB.Ston.StonSerializer serializer)
		{
			var r = new Record (manager);
			foreach (var e in dictionary) {
				r [e.Key] = e.Value;
			}
			return r;
		}

		public override IDictionary<string, object> Serialize (object obj, Yavit.StellaDB.Ston.StonSerializer serializer)
		{
			return (IDictionary<string, object>)obj;
		}

		static readonly IEnumerable<Type> types = Array.AsReadOnly(new [] {typeof(Record)});
		public override IEnumerable<Type> SupportedTypes {
			get {
				return types;
			}
		}
	}
}


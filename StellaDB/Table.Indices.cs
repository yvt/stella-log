using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB
{
	public partial class Table
	{
		static readonly Ston.StonSerializer indexInfoSerializer = new Ston.StonSerializer();
		static Table() {
			indexInfoSerializer.RegisterConverters (new Ston.StonConverter[]{
				new Indexer.IndexConverter ()
			});
		}

		sealed class TableIndex
		{
			public readonly Table Table;
			public readonly Indexer.Index Index;
			public readonly LowLevel.BTree Store;

			// Assigned by ComputeKey.
			public byte[] ComputedKey;

			// Temporary variable. Not used by TableIndex itself.
			public byte[] OldComputedKey;
			public bool HasOldComputedKey;

			// Opens index.
			public TableIndex(Table table, long indexId, byte[] info)
			{
				Table = table;

				var iparam = indexInfoSerializer.Deserialize<Indexer.IndexParameters>(info);
				Index = (Indexer.Index)iparam.CreateKeyProvider();

				Store = table.database.LowLevelDatabase.OpenBTree(indexId, Index.KeyComparer);
			}

			// Creates index.
			public TableIndex(Table table, Indexer.Index index)
			{
				Table = table;
				Index = index;

				var param = new LowLevel.BTreeParameters();
				param.MaximumKeyLength = Index.KeyLength;
				Store = table.database.LowLevelDatabase.CreateBTree(param, Index.KeyComparer);
			}

			public string[] GetEntryNames()
			{
				return (from field in Index.GetFields ()
					select field.Name).ToArray();
			}

			public byte[] GetInfo()
			{
				return indexInfoSerializer.Serialize (Index.Parameters);
			}

			public long IndexId
			{
				get { return Store.BlockId; }
			}

			// Computes a key and stores it to ComputedKey.
			public bool ComputeKey(Indexer.Index.Row row)
			{
				if (ComputedKey == null) {
					ComputedKey = new byte[Index.KeyLength];
					OldComputedKey = new byte[Index.KeyLength];
				}
				return Index.EncodeKey (row, ComputedKey, 0);
			}

			public void DeleteKey(byte[] key)
			{
				Store.DeleteEntry(key);
			}

			public void InsertKey(byte[] key)
			{
				Store.InsertEntry (key);
			}

			public void Drop()
			{
				Store.Drop ();
			}
		}

		public class IndexEntry
		{
			readonly string name;
			readonly Indexer.KeyParameter keyParameter;

			public string Name {
				get { return name; }
			}

			public Indexer.KeyParameter KeyParameter {
				get { return keyParameter; }
			}

			public IndexEntry(string name, Indexer.KeyParameter param)
			{
				this.name = name;
				this.keyParameter = param;
			}

			public static IndexEntry CreateNumericIndexEntry(string name)
			{
				return new IndexEntry (name, new Indexer.NumericKeyParameters ());
			}
			public static IndexEntry CreateBinaryIndexEntry(string name, int maximumKeyLength)
			{
				return new IndexEntry (name, new Indexer.BinaryKeyParameters() { KeyLength = maximumKeyLength });
			}
		}

		Dictionary<string[], TableIndex> indices = 
			new Dictionary<string[], TableIndex>(Utils.ArrayComparer<string>.Default);

		Indexer.QueryOptimizer optimizer;

		void InvalidateQueryOptimizer()
		{
			optimizer = null;
		}

		sealed class QOIndex: Indexer.QueryOptimizer.Index
		{
			public readonly TableIndex TableIndex;
			public QOIndex(TableIndex index, double cardinality):
			base(index.Index, cardinality)
			{
				TableIndex = index;
			}
		}

		void EnsureQueryOptimizer()
		{
			if (optimizer != null) {
				return;
			}

			EnsureLoaded ();

			optimizer = new Indexer.QueryOptimizer ();
			foreach (var idx in indices) {
				// FIXME: cardinality is currently approximated by field count
				optimizer.RegisterIndex (new QOIndex(idx.Value, idx.Value.Index.GetFields().Count()));
			}
		}

		void LoadIndices()
		{
			indices.Clear ();
			if (store == null) {
				// Table is not materialized
				return;
			}
			optimizer = null;

			foreach (var keyinfo in database.MasterTable.GetIndicesOfTable(store.BlockId)) {
				var idx = new TableIndex (this, keyinfo.IndexId, keyinfo.Info);
				indices.Add (idx.GetEntryNames (), idx);
			}
		}

		public void EnsureIndex(IndexEntry[] entries)
		{
			if (entries == null)
				throw new ArgumentNullException ("entries");
			if (entries.Length == 0)
				throw new ArgumentException ("No entries given.");
			if (entries.Any (e => e == null))
				throw new ArgumentException ("One of the entries is null.");
			if (entries.Any (e => e.KeyParameter == null))
				throw new ArgumentException ("THe key parameter of one of the entries is null.");
			if (entries.Any (e => string.IsNullOrEmpty (e.Name)))
				throw new ArgumentException ("THe name of one of the entries is null or empty.");

			EnsureStoreCreated ();

			// Does index already exist?
			var names = (from entry in entries
				select entry.Name).ToArray();
			if (indices.ContainsKey(names)) {
				return;
			}

			// Create index.
			var ientries = from entry in entries
			               select new Indexer.Index.Field () {
				Name = entry.Name,
				KeyProvider = entry.KeyParameter.CreateKeyProvider()
			};

			var idx = new Indexer.Index (ientries);
			var tidx = new TableIndex (this, idx);

			indices.Add (tidx.GetEntryNames (), tidx);
			InvalidateQueryOptimizer ();
			database.MasterTable.AddIndexToTable (TableId, tidx.IndexId, tidx.GetInfo());

			// Add items to the index.
			foreach (var row in store) {
				try {
					var reader = new Ston.StonReader (row.ReadValue ());
					var val = new Ston.SerializedStonVariant (reader);
					var r = new Indexer.Index.Row () {
						RowId = DecodeRowIdForKey(row.GetKey()),
						Value = val
					};
					if (tidx.ComputeKey(r)) {
						tidx.InsertKey (tidx.ComputedKey);
					}
				} 
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }
			}

		}

		public void RemoveIndex(string[] entryNames)
		{
			if (entryNames.Length == 0 || entryNames == null)
				throw new ArgumentException ("entryNames");
			if (entryNames.Any(string.IsNullOrEmpty))
				throw new ArgumentException ("One of the entry name is empty or null.", "entryNames");

			EnsureLoaded ();

			TableIndex tidx;
			if (indices.TryGetValue(entryNames, out tidx)) {
				database.MasterTable.RemoveIndexFromTable (TableId, tidx.IndexId);
				tidx.Drop ();
				indices.Remove (entryNames);
				InvalidateQueryOptimizer ();
			}
		}

		public void RemoveAllIndices()
		{
			EnsureLoaded ();

			foreach (var idx in indices) {
				database.MasterTable.RemoveIndexFromTable (TableId, idx.Value.IndexId);
				idx.Value.Drop ();
			}
			indices.Clear ();

			InvalidateQueryOptimizer ();
		}

		void InsertRowToIndex(long rowId, Ston.StonVariant value)
		{
			var r = new Indexer.Index.Row() {
				RowId = rowId,
				Value = value
			};
			foreach (var idx in indices.Values) {
				try {
					if (idx.ComputeKey(r)) {
						idx.InsertKey(idx.ComputedKey);
					}
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }
			}
		}

		void DeleteRowFromIndex(long rowId, Ston.StonVariant value)
		{
			var r = new Indexer.Index.Row() {
				RowId = rowId,
				Value = value
			};
			foreach (var idx in indices.Values) {
				try {
					if (idx.ComputeKey(r)) {
						idx.DeleteKey(idx.ComputedKey);
					}
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }
			}
		}

		void PrepareIndexBeforeUpdatingRow(long rowId, Ston.StonVariant value)
		{
			var r = new Indexer.Index.Row() {
				RowId = rowId,
				Value = value
			};
			foreach (var idx in indices.Values) {
				try {
					if (idx.ComputeKey(r)) {
						idx.HasOldComputedKey = true;
						Buffer.BlockCopy(idx.ComputedKey, 0,
							idx.OldComputedKey, 0, idx.ComputedKey.Length);
					} else {
						idx.HasOldComputedKey = false;
					}
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }
			}
		}

		void UpdateIndexAfterUpdatingRow(long rowId, Ston.StonVariant value)
		{
			var r = new Indexer.Index.Row() {
				RowId = rowId,
				Value = value
			};
			var cmp = DefaultKeyComparer.Instance;
			foreach (var idx in indices.Values) {
				try {
					if (idx.ComputeKey(r)) {
						if (!idx.HasOldComputedKey) {
							idx.InsertKey(idx.ComputedKey);
						} else if (!cmp.Equals(idx.ComputedKey, idx.OldComputedKey)) {
							idx.DeleteKey(idx.OldComputedKey);
							idx.InsertKey(idx.ComputedKey);
						}
					} else {
						if (idx.HasOldComputedKey) {
							idx.DeleteKey(idx.OldComputedKey);
						}
					}
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }
			}
		}

	}
}


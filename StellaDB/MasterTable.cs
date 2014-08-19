using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB
{
	internal sealed class MasterTable
	{
		readonly Database database;

		enum ObjectType: byte
		{
			Table = 1,
			Index = 2
		}

		LowLevel.BTree store;
	
		public MasterTable (Database database)
		{
			this.database = database;
			if (database == null)
				throw new ArgumentNullException ("database");
		}

		internal void Unload()
		{
			store = null;
		}

		void EnsureLoaded()
		{
			if (store != null) {
				return;
			}

			var lldb = database.LowLevelDatabase;
			if (lldb.UserBlockId1 == 0) {
				var param = new LowLevel.BTreeParameters ();
				param.MaximumKeyLength = 64;

				store = lldb.CreateBTree (param);
				lldb.UserBlockId1 = store.BlockId;
			} else {
				store = lldb.OpenBTree (lldb.UserBlockId1);
			}
		}

		int MaximumTableNameInKey
		{
			get {
				return store.MaximumKeyLength - 9;
			}
		}

		byte[] MakeTableKey(byte[] name, long tableId)
		{
			int inkeyLen = Math.Min (name.Length, MaximumTableNameInKey);
			byte[] key = new byte[inkeyLen + 9];
			Buffer.BlockCopy (name, 0, key, 1, inkeyLen);
			key [0] = (byte)ObjectType.Table;
			new InternalUtils.BitConverter (key).Set (key.Length - 8, tableId);
			return key;
		}

		public long? GetTableIdByName(byte[] name)
		{
			if (name == null || name.Length == 0) {
				throw new ArgumentException ("Table name is empty or null.");
			}

			EnsureLoaded ();

			// Structure of Table Key:
			// 0                 - ObjectType.Table
			// 1       ~ len - 9 - Table name (until it fits)
			// len - 8 ~ len - 1 - Table Id
			// Remaining part of the name is stored as the value.
			byte[] key = MakeTableKey(name, 0);

			var comparer = store.KeyComparer;
			var maxLen = store.MaximumKeyLength;
			var maxTableNameLen = MaximumTableNameInKey;

			foreach (var row in store.EnumerateEntiresInAscendingOrder(key)) {
				var rowKey = row.GetKey ();
				// Equals (except Table Id);
				if (comparer.Compare(rowKey, 0, Math.Max(rowKey.Length - 8, 0),
					key, 0, key.Length - 8) != 0) {
					break;
				}
					
				if (rowKey.Length == maxLen) {
					var remainingPart = row.ReadValue ();
					if (comparer.Compare(name, maxTableNameLen, name.Length - maxTableNameLen,
						remainingPart, 0, remainingPart.Length) != 0) {
						continue;
					}
				}

				return new InternalUtils.BitConverter (rowKey).GetInt64 (rowKey.Length - 8);
			}

			return null;
		}

		public void AddTable(byte[] name, long tableId)
		{
			if (name == null || name.Length == 0) {
				throw new ArgumentException ("Table name is empty or null.");
			}

			EnsureLoaded ();

			var maxTableNameLen = MaximumTableNameInKey;

			var row = store.InsertEntry (MakeTableKey(name, tableId));
			if (name.Length > maxTableNameLen) {
				byte[] remainingPart = new byte[name.Length - maxTableNameLen];
				Buffer.BlockCopy (name, maxTableNameLen, remainingPart, 0, name.Length - maxTableNameLen);
				row.WriteValue (remainingPart);
			}
		}

		public void RemoveTable(byte[] name, long tableId)
		{
			if (name == null || name.Length == 0) {
				throw new ArgumentException ("Table name is empty or null.");
			}

			EnsureLoaded ();

			store.DeleteEntry (MakeTableKey (name, tableId));
		}

		byte[] MakeIndexKey(long tableId, long indexId)
		{
			byte[] key = new byte[17];
			var bc = new InternalUtils.BitConverter (key);
			key [0] = (byte)ObjectType.Index;
			bc.Set (1, tableId);
			bc.Set (9, indexId);
			return key;
		}

		public void AddIndexToTable(long tableId, long indexId, byte[] info)
		{
			EnsureLoaded ();

			var row = store.InsertEntry (MakeIndexKey (tableId, indexId));
			row.WriteValue (info);
		}

		public void RemoveIndexFromTable(long tableId, long indexId)
		{
			EnsureLoaded ();
			store.DeleteEntry (MakeIndexKey (tableId, indexId));
		}

		public struct IndexInfo
		{
			public long IndexId;
			public byte[] Info;
		}
		public IEnumerable<IndexInfo> GetIndicesOfTable(long tableId)
		{
			EnsureLoaded ();

			var ret = new List<IndexInfo> ();
			var startKey = MakeIndexKey (tableId, 0);
			var comparer = store.KeyComparer;

			foreach (var row in store.EnumerateEntiresInAscendingOrder(startKey)) {
				var key = row.GetKey ();
				if (key.Length != 17 || comparer.Compare(key, 0, 9, startKey, 0, 9) != 0) {
					break;
				}

				var iId = new InternalUtils.BitConverter (key).GetInt64 (9);
				ret.Add (new IndexInfo () {
					IndexId = iId,
					Info = row.ReadValue()
				});
			}

			return ret;
		}


	}
}


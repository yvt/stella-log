using System;
using System.Linq;
using System.Collections.Generic;

namespace Yavit.StellaDB
{
	public partial class Table
	{
		Database database;
		readonly string tableName;
		readonly byte[] tableNameBytes;
		bool loaded = false;

		readonly byte[] rowIdBuffer;

		LowLevel.BTree store;

		internal Table (Database db, string tableName)
		{
			this.database = db;
			this.tableName = tableName;
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException ("tableName");

			tableNameBytes = new System.Text.UTF8Encoding ().GetBytes (tableName);
			rowIdBuffer = new byte[8];
		}

		#region Load/Unload
		void EnsureLoaded()
		{
			if (loaded) {
				return;
			}

			LoadStore ();
			LoadIndices ();
			loaded = true;
		}


		internal void Unload()
		{
			loaded = false;
		}
		#endregion

		#region Store Creation

		void LoadStore()
		{
			var tableId = database.MasterTable.GetTableIdByName (tableNameBytes);
			if (tableId == null) {
				store = null;
			} else {
				store = database.LowLevelDatabase.OpenBTree ((long)tableId, ReversedKeyComparer.Instance);
			}
		}
		// Called when new row or index is being added
		void EnsureStoreCreated()
		{
			EnsureLoaded ();

			if (store == null) {
				var param = new LowLevel.BTreeParameters ();
				param.MaximumKeyLength = 8; // Row Id
				store = database.LowLevelDatabase.CreateBTree (param, ReversedKeyComparer.Instance);
				database.MasterTable.AddTable (tableNameBytes, store.BlockId);

				// Reset auto increment row value
				AutoIncrementRowIdValue = 1;
			}
		}
		#endregion

		long DecodeRowIdForKey(byte[] key)
		{
			return new InternalUtils.BitConverter (key).GetInt64 (0);
		}

		void EncodeRowId(byte[] buffer, long rowId)
		{
			new InternalUtils.BitConverter (buffer).Set (0, rowId);
		}

		long TableId
		{
			get {
				return store.BlockId;
			}
		}

		public long AutoIncrementRowIdValue
		{
			get {
				EnsureLoaded ();

				if (store == null) {
					return 1;
				} else {
					return store.UserInfo1;
				}
			}
			set {
				EnsureStoreCreated ();
				store.UserInfo1 = value;
			}
		}

		public void Drop()
		{
			EnsureLoaded ();

			if (store == null) {
				return;
			}

			RemoveAllIndices ();
			store.Drop ();
			store = null;

			Unload ();
		}


	}
}


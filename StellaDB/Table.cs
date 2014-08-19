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

		LowLevel.BTree store;

		internal Table (Database db, string tableName)
		{
			this.database = db;
			this.tableName = tableName;
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException ("tableName");

			tableNameBytes = new System.Text.UTF8Encoding ().GetBytes (tableName);
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
				// TODO: need to specify ReverseKeyComparer
				store = database.LowLevelDatabase.OpenBTree ((long)tableId);
			}
		}
		// Called when new row or index is being added
		void EnsureStoreCreated()
		{
			EnsureLoaded ();

			if (store == null) {
				var param = new LowLevel.BTreeParameters ();
				param.MaximumKeyLength = 8; // Row Id
				store = database.LowLevelDatabase.CreateBTree (param);
				// TODO: need to specify ReverseKeyComparer
				database.MasterTable.AddTable (tableNameBytes, store.BlockId);

				// Reset auto increment row value
				AutoIncrementRowIdValue = 1;
			}
		}
		#endregion

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

			// TODO: Drop
		}

	}
}


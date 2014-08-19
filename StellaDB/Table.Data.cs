using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB
{
	public partial class Table
	{

		void InternalInsertRaw(long rowId, byte[] value, bool updateOnDuplicate, bool errorOnNotFound)
		{
			EnsureStoreCreated ();

			EncodeRowId (rowIdBuffer, rowId);
			LowLevel.IKeyValueStoreEntry entry;

			if ((entry = store.FindEntry (rowIdBuffer)) != null) {
				if (updateOnDuplicate) {
					try {
						var reader = new Ston.StonReader (entry.ReadValue());
						var val = new Ston.SerializedStonVariant (reader);
						PrepareIndexBeforeUpdatingRow(rowId, val);
					}
					catch (Ston.StonException) { }
					catch (Ston.StonVariantException) { }

					entry.WriteValue (value);

					try {
						var reader = new Ston.StonReader (value);
						var val = new Ston.SerializedStonVariant (reader);
						UpdateIndexAfterUpdatingRow(rowId, val);
					}
					catch (Ston.StonException) { }
					catch (Ston.StonVariantException) { }
				} else {
					throw new InvalidOperationException ("Row with the specified row ID already exists.");
				}
			}

			if (errorOnNotFound) {
				throw new InvalidOperationException ("Specified row ID was not found.");
			}

			store.InsertEntry (rowIdBuffer).WriteValue (value);

			try {
				var reader = new Ston.StonReader (value);
				var val = new Ston.SerializedStonVariant (reader);
				InsertRowToIndex(rowId, val);
			}
			catch (Ston.StonException) { }
			catch (Ston.StonVariantException) { }
		}

		public void InsertRaw(long rowId, byte[] value, bool updateOnDuplicate)
		{
			InternalInsertRaw (rowId, value, updateOnDuplicate, false);
		}

		public void InsertRaw(byte[] value, bool updateOnDuplicate)
		{
			InsertRaw (AutoIncrementRowIdValue, value, updateOnDuplicate);
			++AutoIncrementRowIdValue;
		}

		public void UpdateRaw(long rowId, byte[] value)
		{
			InternalInsertRaw (rowId, value, true, true);
		}

		public void Delete(long rowId)
		{
			EnsureStoreCreated ();

			EncodeRowId (rowIdBuffer, rowId);

			var r = store.FindEntry (rowIdBuffer);
			if (r != null) {
				try {
					var reader = new Ston.StonReader (r.ReadValue());
					var val = new Ston.SerializedStonVariant (reader);
					DeleteRowFromIndex(rowId, val);
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }

				store.DeleteEntry (rowIdBuffer);
			}
		}



	}
}


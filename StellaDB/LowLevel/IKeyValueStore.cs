using System;
using System.IO;
using System.Collections.Generic;

namespace Yavit.StellaDB.LowLevel
{
	public interface IKeyValueStoreEntry
	{
		byte[] GetKey();
		byte[] ReadValue();
		void WriteValue(byte[] buffer);

		Stream OpenValueStream();
	}
	public interface IKeyValueStore: IEnumerable<IKeyValueStoreEntry>
	{
		IKeyValueStoreEntry FindEntry(byte[] key);
		IKeyValueStoreEntry InsertEntry(byte[] key);
		bool DeleteEntry(byte[] key);
	}

	public interface IOrderedKeyValueStore: IKeyValueStore
	{
		IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInAscendingOrder();
		IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInDescendingOrder();
		IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInAscendingOrder(byte[] startPoint);
		IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInDescendingOrder(byte[] startPoint);
	}
}


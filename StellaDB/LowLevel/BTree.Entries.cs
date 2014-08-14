
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.LowLevel
{
	public partial class BTree
	{
		Entry activeEntry = null;

		Entry GetEntryIfOnMemoryByKey(byte[] key)
		{
			if (activeEntry != null &&
				comparer.Compare(activeEntry.key, 0, activeEntry.key.Length,
					key, 0, key.Length) == 0) {
				return activeEntry;
			}
			return null;
		}

		Entry GetEntryIfOnMemoryByIndex(int index)
		{
			if (activeEntry != null &&
				activeEntry.IsAtCursorTop &&
				activeEntry.Index == index) {
				return activeEntry;
			}
			return null;
		}

		// Cleans the currently selected entry up before deleting it.
		// You can provide the key to reduce dynamic allocation.
		void CleanupCurrentEntry(int index) 
		{
			var item = cursor.Top.GetItem (index);

			FlushAllEntriesForStructureModification ();

			if (item.ValueLength == 0 && item.OverflowPageBlockId != 0) {
				// BLOB
				Database.OpenBlob (item.OverflowPageBlockId).Drop ();
				item.OverflowPageBlockId = 0;
			}
		}

		void CheckNotDropped()
		{
			if (headerBlockId == 0) {
				throw new ObjectDisposedException ("BTree", "Tree was dropped.");
			}
		}

		public void Drop()
		{
			CheckNotDropped ();
			FlushAllEntriesForStructureModification ();
			if (cursor != null) {
				NodeBlock.DropTree (cursor);
			}
			cursor = null;
			header.Drop ();
			headerBlockId = 0;
		}

		void FlushAllEntriesForStructureModification()
		{
			SetActiveEntry (null);
		}

		void SetActiveEntry(Entry e)
		{
			if (activeEntry == e) {
				return;
			}
			if (activeEntry != null) {
				activeEntry.FlushForStructureModification ();
			}
			activeEntry = e;
		}

		public class Entry: IKeyValueStoreEntry
		{
			BTree tree;
			int cursorDepth;
			long nodeBlockId;
			int itemIndex;
			int treeStateNumber;
			NodeBlock node;

			// Updated whenever BLOB is attached (as an overflow page) or detached.
			int entryStateNumber = 0; 

			internal byte[] key;

			internal Entry(BTree tree, int itemIndex)
			{
				this.tree = tree;
				SetupFromCursorTop(itemIndex);
				this.key = tree.cursor.Top.GetItem(itemIndex).GetKey();
				SetNeedsFlushForStructureModification();
			}

			private void SetupFromCursorTop(int newItemIndex)
			{
				var cursor = tree.cursor;
				node = cursor.Top;
				cursorDepth = cursor.ActiveLevel;
				nodeBlockId = (long)node.BlockId;
				this.itemIndex = newItemIndex;
				treeStateNumber = tree.treeStateNumber;
				++entryStateNumber;
				SetNeedsFlushForStructureModification ();
			}

			public bool IsAtCursorTop
			{
				get {
					return tree.cursor.Top.BlockId == nodeBlockId;
				}
			}
			public int Index
			{
				get {
					return itemIndex;
				}
			}

			void SetNeedsFlushForStructureModification()
			{
				tree.SetActiveEntry (this);
			}

			public void FlushForStructureModification()
			{
				Flush ();
				node = null;
			}

			public void Flush()
			{
				if (node != null) {
					node.Write ();
				}
			}

			public void Deleted()
			{
				key = null;
				node = null;
				++entryStateNumber;
			}

			private void CheckNotDeleted() 
			{
				if (key == null) {
					throw new ObjectDisposedException ("Entry", "Entry was deleted.");
				}
			}

			void CheckState()
			{
				var cursor = tree.cursor;
				CheckNotDeleted ();
				if (treeStateNumber != tree.treeStateNumber) {
					node = null;

					// We need to search again because tree structure has changed.
					tree.DisactivateAllEnumerator ();
					var result = NodeBlock.FindNode (tree.cursor, key, 0, key.Length);
					if (result.ExactMatch == false) {
						throw new ObjectDisposedException ("Entry", "Entry was deleted.");
					}

					SetupFromCursorTop (result.NextIndex);
				}

				if (node == null) {
					if (cursorDepth <= cursor.ActiveLevel &&
						cursor.Nodes [cursorDepth].BlockId == nodeBlockId) {
						node = cursor.Nodes [cursorDepth];
					} else {
						var n = new NodeBlock (tree);
						n.LoadForAccessingItem (nodeBlockId);
						node = n;
					}
					++entryStateNumber;
					SetNeedsFlushForStructureModification ();
				}

			}

			NodeBlock Node
			{
				get {
					CheckState ();
					return node;
				}
			}

			NodeBlock.Item NodeItem
			{
				get {
					return Node.GetItem (itemIndex);
				}
			}

			public byte[] GetKey ()
			{
				CheckNotDeleted ();
				return (byte[])key.Clone ();
			}

			public byte[] ReadValue ()
			{
				var item = NodeItem;
				if (item.ValueLength == 0) {
					if (item.OverflowPageBlockId == 0) {
						return new byte[] { };
					} else {
						return tree.db.OpenBlob (item.OverflowPageBlockId).ReadAllBytes ();
					}
				} else {
					return item.GetInTreeValue ();
				}
			}

			public void WriteValue (byte[] buffer)
			{
				if (buffer == null) {
					throw new ArgumentNullException ("buffer");
				}

				var item = NodeItem;
				if (item.ValueLength == 0 && item.OverflowPageBlockId != 0) {
					tree.db.OpenBlob (item.OverflowPageBlockId).WriteAllBytes (buffer);
				} else {
					if (buffer.Length > item.MaxValueLength) {
						SwitchToBlob ();
						WriteValue (buffer);
					} else {
						item.ValueLength = buffer.Length;
						Buffer.BlockCopy (buffer, 0, node.Bytes, item.ValueOffset, buffer.Length);
					}
				}
				Flush ();
			}

			void SwitchToBlob()
			{
				var item = NodeItem;
				if (item.ValueLength == 0 &&
					item.OverflowPageBlockId != 0) {
					return;
				}

				var blob = tree.Database.CreateBlob ();
				blob.WriteAllBytes (ReadValue ());
				item.ValueLength = 0;
				item.OverflowPageBlockId = blob.BlockId;
				Node.Write ();

				++entryStateNumber;
			}

			public Stream OpenValueStream ()
			{
				CheckNotDeleted ();
				return new EntryStream (this);
			}

			sealed class EntryStream: Stream
			{
				Entry entry;
				int entryStateNumber;
				Stream blobStream;
				long position = 0;

				public EntryStream(Entry entry)
				{
					this.entry = entry;
					entryStateNumber = entry.entryStateNumber - 1;
				}

				void SyncEntryState()
				{
					if (entry == null) {
						throw new ObjectDisposedException ("EntryStream", "Stream is closed.");
					}
					entry.CheckState ();
					if (entryStateNumber == entry.entryStateNumber) {
						return;
					}
					if (blobStream != null) {
						blobStream.Dispose ();
						blobStream = null;
					}

					var item = entry.NodeItem;
					if (item.ValueLength == 0 && item.OverflowPageBlockId != 0) {
						blobStream = entry.tree.Database.OpenBlob (item.OverflowPageBlockId).OpenStream ();
						blobStream.Position = position;
					}

					entryStateNumber = entry.entryStateNumber;
				}

				protected override void Dispose (bool disposing)
				{
					if (blobStream != null) {
						blobStream.Dispose ();
						blobStream = null;
					}
					--entryStateNumber;
					entry = null;
					base.Dispose (disposing);
				}

				public override void Flush ()
				{
					SyncEntryState ();
					if (blobStream != null) {
						blobStream.Flush ();
					}
					if (entry.node != null) {
						entry.node.Write ();
					}
				}

				public override int Read (byte[] buffer, int offset, int count)
				{
					if (buffer == null)
						throw new ArgumentNullException ("buffer");
					if (offset < 0)
						throw new ArgumentOutOfRangeException ("offset");
					if (offset + count > buffer.Length)
						throw new ArgumentOutOfRangeException ("count");

					SyncEntryState ();

					int readCount = 0;
					if (blobStream != null) {
						readCount = blobStream.Read (buffer, offset, count);
					} else {
						var item = entry.NodeItem;
						if (position < (long)item.ValueLength) {
							count = Math.Min (count, item.ValueLength - (int)position);
							Buffer.BlockCopy (entry.node.Bytes, item.ValueOffset + (int)position,
								buffer, offset, count);
							readCount = count;
						}
					}

					position += readCount;
					return readCount;
				}

				public override void Write (byte[] buffer, int offset, int count)
				{
					if (buffer == null)
						throw new ArgumentNullException ("buffer");
					if (offset < 0)
						throw new ArgumentOutOfRangeException ("offset");
					if (offset + count > buffer.Length)
						throw new ArgumentOutOfRangeException ("count");

					SyncEntryState ();

					// we might have to switch to blob if stream will become too long...
					if (blobStream == null) {
						long newLen = checked(position + count);
						if (newLen > (int)entry.NodeItem.MaxValueLength) {
							entry.SwitchToBlob ();
							SyncEntryState ();
						} else {
							var item = entry.NodeItem;
							item.ValueLength = (int)newLen;
						}
					}

					if (blobStream != null) {
						blobStream.Write (buffer, offset, count);
					} else {
						var item = entry.NodeItem;
						Buffer.BlockCopy (buffer, offset, entry.node.Bytes,
							item.ValueOffset + (int)position, count);
						entry.node.Dirty = true;
					}

					position = checked(position + count);
				}

				public override long Seek (long offset, SeekOrigin origin)
				{
					long newPos = position;
					switch (origin) {
					case SeekOrigin.Begin:
						newPos = offset;
						break;
					case SeekOrigin.Current:
						newPos += offset;
						break;
					case SeekOrigin.End:
						newPos = Length + offset;
						break;
					}
					Position = newPos;
					return Position;
				}

				public override void SetLength (long value)
				{
					if (value < 0)
						throw new ArgumentOutOfRangeException ("value");

					SyncEntryState ();

					if (blobStream != null) {
						blobStream.SetLength (value);
					} else {
						var item = entry.NodeItem;
						var maxInTreeLen = item.MaxValueLength;
						if (value > (int)maxInTreeLen) {
							entry.SwitchToBlob ();
							SyncEntryState ();
							SetLength (value);
						} else {
							var curLen = item.ValueLength;
							var newLen = (int)value;
							if (newLen > curLen) {
								InternalUtils.ZeroFill (entry.node.Bytes, item.ValueOffset + curLen,
									newLen - curLen);
							}
							item.ValueLength = newLen;
							if (value == 0) {
								item.OverflowPageBlockId = 0;
							}
						}
					}
				}

				public override bool CanRead {
					get { return true; }
				}

				public override bool CanSeek {
					get { return true; }
				}

				public override bool CanWrite {
					get { return true; }
				}

				public override long Length {
					get {
						SyncEntryState ();
						if (blobStream != null) {
							return blobStream.Length;
						} else {
							return entry.NodeItem.ValueLength;
						}
					}
				}

				public override long Position {
					get { return position; }
					set {
						if (value < 0) {
							throw new ArgumentOutOfRangeException ("value");
						}
						position = value;
						if (blobStream != null) {
							blobStream.Position = position;
						}
					}
				}

			} // EntryStream

		} // Entry

		#region IKeyValueStore implementation
		public IKeyValueStoreEntry FindEntry (byte[] key)
		{
			if (key == null) {
				throw new ArgumentNullException ("key");
			}
			CheckNotDropped ();
			if (cursor == null) {
				return null;
			}

			Entry entry = GetEntryIfOnMemoryByKey(key);
			if (entry != null) {
				return entry;
			}

			DisactivateAllEnumerator ();
			var result = NodeBlock.FindNode (cursor, key, 0, key.Length);
			if (result.ExactMatch) {
				return new Entry (this, result.NextIndex);
			}
			return null;
		}
		public bool DeleteEntry(byte[] key)
		{
			if (key == null) {
				throw new ArgumentNullException ("key");
			}
			CheckNotDropped ();
			if (cursor == null) {
				return false;
			}

			DisactivateAllEnumerator ();
			var result = NodeBlock.FindNode (cursor, key, 0, key.Length);
			if (result.ExactMatch) {
				CleanupCurrentEntry (result.NextIndex);
				NodeBlock.DeleteKey (result.NextIndex, cursor);

				Flush ();
				return true;
			}
			return false;
		}
		public IKeyValueStoreEntry InsertEntry (byte[] key)
		{
			if (key == null) {
				throw new ArgumentNullException ("key");
			}
			CheckNotDropped ();
			if (cursor == null) {
				// Tree is empty. Initialize the tree.
				var root = new NodeBlock (this);
				root.InitializeEmptyRoot (db.Freemap.AllocateBlock ());
				header.RootNodeBlockId = (long)root.BlockId;
				cursor = new NodeCursor (root);
				header.Write ();
			}

			Entry entry = GetEntryIfOnMemoryByKey(key);
			if (entry != null) {
				return entry;
			}

			if (!comparer.IsValidKey(key)) {
				throw new InvalidOperationException ("Invalid key.");
			}

			DisactivateAllEnumerator ();
			var result = NodeBlock.FindNode (cursor, key, 0, key.Length);
			if (result.ExactMatch) {
				return new Entry (this, result.NextIndex);
			}

			cursor.Nodes [0].Validate ();
			NodeBlock.InsertKey (result, cursor, key, 0, key.Length);
			Flush ();
			cursor.Nodes [0].Validate ();

			result = NodeBlock.FindNode (cursor, key, 0, key.Length);
			if (!result.ExactMatch) {
				throw new InvalidOperationException ();
			}
			var e = new Entry (this, result.NextIndex);
			SetActiveEntry (e);
			return e;
		}
		#endregion
	}
}

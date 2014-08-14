

using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.LowLevel
{
	public partial class BTree
	{

		// Call after cursor was moved
		void DisactivateAllEnumerator()
		{
			if (activeEnumerator != null) {
				activeEnumerator.Disactivate ();
				activeEnumerator = null;
			}
		}

		Enumerator activeEnumerator;

		sealed class Enumerator: IEnumerator<IKeyValueStoreEntry>
		{
			readonly BTree tree;
			byte[] key; // enumerates items where item >= (or >, depending on inclusive) key.
			int index = 0;
			bool started = false; // initial MoveNext done.
			bool active = false;
			bool valid = true;
			bool reachedEnd = false;
			bool descend = false;

			byte[] initialKey;

			// Returns whether enumerator is controlling cursor to do enumeration.
			bool IsActive {
				get { return active; }
			}

			bool IsValid {
				get { return valid && tree.headerBlockId != 0; }
			}

			void EnsureValid() {
				if (!IsValid) {
					throw new ObjectDisposedException ("Enumerator", "Enumerator was disposed.");
				}
			}

			void CheckEof() {
				if (reachedEnd) {
					throw new InvalidOperationException ("Reached the end.");
				}
			}

			NodeBlock.Item Item {
				get {
					return tree.cursor.Top.GetItem (index);
				}
			}

			public Enumerator(BTree tree, bool descend) {
				this.tree = tree;
				this.descend = descend;
				initialKey = null;
				Reset();
			}
			public Enumerator(BTree tree, byte[] key, bool descend) {
				this.tree = tree;
				this.descend = descend;
				initialKey = key;
				Reset();
			}

			public void Disactivate() {
				active = false;
			}

			void Activate() {
				EnsureValid();
				if (reachedEnd) {
					return;
				}
				if (active) {
					return;
				}

				var cursor = tree.cursor;
				if (cursor == null) {
					reachedEnd = true;
					active = false;
					valid = false;
					return;
				}

				if (key == null) {
					cursor.ActiveLevel = 0;
					if (descend) {
						index = NodeBlock.FindGreatestItem (cursor);
					} else {
						index = NodeBlock.FindLeastItem (cursor);
					}
				} else {
					var result = NodeBlock.FindNode (cursor, key, 0, key.Length);
					if (descend) {
						index = result.PrevIndex;
						if (result.ExactMatch) {
							index = result.NextIndex;
						} else if (result.PrevIndex == NodeBlock.InvalidIndex &&
							result.NextIndex != NodeBlock.InvalidIndex) {
							index = NodeBlock.FindPreviousKey (cursor, result.NextIndex);
						}
					} else {
						index = result.NextIndex;
						if (result.NextIndex == NodeBlock.InvalidIndex &&
							result.PrevIndex != NodeBlock.InvalidIndex) {
							index = NodeBlock.FindNextKey (cursor, result.PrevIndex);
						}
					}
				}

				if (index == NodeBlock.InvalidIndex) {
					reachedEnd = true;
					active = false;
					valid = false;
					return;
				}

				key = Item.GetKey ();

				active = true;
			}

			#region IEnumerator implementation

			public bool MoveNext ()
			{

				if (reachedEnd) {
					return false;
				}
				Activate ();

				if (!started) {
					started = true;
				} else {
					if (descend) {
						index = NodeBlock.FindPreviousKey (tree.cursor, index);
					} else {
						index = NodeBlock.FindNextKey (tree.cursor, index);
					}

					if (index == NodeBlock.InvalidIndex) {
						reachedEnd = true;
						active = false;
						valid = false;
						return false;
					}

					key = Item.GetKey ();
				}

				return !reachedEnd;
			}

			public void Reset ()
			{
				EnsureValid ();
				this.key = initialKey;
				reachedEnd = false;
				started = false;
			}

			public IKeyValueStoreEntry Current {
				get {
					EnsureValid ();
					CheckEof ();
					if (!started) {
						throw new InvalidOperationException ("MoveNext is not called yet.");
					}
					var e = tree.GetEntryIfOnMemoryByIndex (index);
					if (e != null) {
						return e;
					}
					e = new Entry (tree, index);
					return e;
				}
			}

			object IEnumerator.Current {
				get {
					return Current;
				}
			}

			#endregion

			#region IDisposable implementation

			public void Dispose ()
			{
				valid = false;
				active = false;
			}

			#endregion

		}

		sealed class EnumeratorWrapper: IEnumerable<IKeyValueStoreEntry>
		{
			Func<Enumerator> builder;
			public EnumeratorWrapper(Func<Enumerator> builder)
			{
				this.builder = builder;
			}
			public IEnumerator<IKeyValueStoreEntry> GetEnumerator ()
			{
				return builder ();
			}
			IEnumerator IEnumerable.GetEnumerator ()
			{
				return builder ();
			}
		}

		#region IOrderedKeyValueStore implementation
		public IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInAscendingOrder ()
		{
			CheckNotDropped ();
			return new EnumeratorWrapper (() => new Enumerator(this, false));
		}
		public IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInDescendingOrder ()
		{
			CheckNotDropped ();
			return new EnumeratorWrapper (() => new Enumerator(this, true));
		}
		public IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInAscendingOrder (byte[] startPoint)
		{
			if (startPoint == null)
				throw new ArgumentNullException ("startPoint");
			CheckNotDropped ();
			byte[] key = (byte[])startPoint.Clone ();
			return new EnumeratorWrapper (() => new Enumerator(this, key, false));
		}
		public IEnumerable<IKeyValueStoreEntry> EnumerateEntiresInDescendingOrder (byte[] startPoint)
		{
			if (startPoint == null)
				throw new ArgumentNullException ("startPoint");
			CheckNotDropped ();
			byte[] key = (byte[])startPoint.Clone ();
			return new EnumeratorWrapper (() => new Enumerator(this, key, true));
		}
		#endregion
		#region IEnumerable implementation
		public IEnumerator<IKeyValueStoreEntry> GetEnumerator ()
		{
			return EnumerateEntiresInAscendingOrder ().GetEnumerator ();
		}
		IEnumerator IEnumerable.GetEnumerator ()
		{
			foreach(var e in EnumerateEntiresInAscendingOrder()) {
				yield return e;
			}
		}
		#endregion
	}
}

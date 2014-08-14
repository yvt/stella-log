//#define Validate

using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.LowLevel
{
	public partial class BTree
	{
		#if Validate
		public bool ValidationEnabled = false;
		#endif
		sealed class HeaderBlock
		{
			readonly BTree tree;

			int maximumEffectiveKeyLength = 96;

			public int MaximumEffectiveKeyLength {
				get {
					return maximumEffectiveKeyLength;
				}
				set {
					if (value > (tree.storage.BlockSize - 16) / 2 - 10) { // see (2)
						throw new ArgumentOutOfRangeException ("value", "Key length must be less than the block size - 36.");
					} else if (value <= 0) {
						throw new ArgumentOutOfRangeException ("value", "Key length must be positive.");
					}
					maximumEffectiveKeyLength = value;
				}
			}

			// Root node block id which is zero for empty tree
			public long RootNodeBlockId = 0;

			public HeaderBlock(BTree tree)
			{
				this.tree = tree;
			}

			public void Load()
			{
				using (var handle = tree.Database.BufferPool.CreateHandle()) {
					tree.storage.ReadBlock (tree.BlockId, handle.Buffer, 0);

					var str = handle.Stream;
					var br = handle.BinaryReader;

					str.Seek (0, SeekOrigin.Begin);

					uint magic = br.ReadUInt32 ();
					if (magic != HeaderMagic) {
						throw new InvalidMagicNumberException();
					}

					uint version = br.ReadUInt32 ();
					if (version != CurrentVersion) {
						throw new InvalidFormatVersionException ();
					}

					try {
						MaximumEffectiveKeyLength = br.ReadUInt16 ();
					} catch (ArgumentOutOfRangeException ex) {
						throw new DataInconsistencyException (ex);
					}

					RootNodeBlockId = (long)br.ReadUInt64 ();
					if (RootNodeBlockId <= 0) {
						throw new DataInconsistencyException ("Root Block ID is not positive.");
					}
				}
			}

			public void Drop()
			{
				tree.db.Freemap.DeallocateBlock (tree.BlockId);
			}

			public void Write()
			{
				using (var handle = tree.Database.BufferPool.CreateHandle()) {
					var str = handle.Stream;
					var bw = handle.BinaryWriter;

					str.Seek (0, SeekOrigin.Begin);
					bw.Write (HeaderMagic);
					bw.Write (CurrentVersion);
					bw.Write (checked((ushort)MaximumEffectiveKeyLength));
					bw.Write(RootNodeBlockId);

					tree.storage.WriteBlock (tree.BlockId, handle.Buffer, 0);
				}
			}

		}

		[Flags] enum NodeFlags
		{
			None = 0,
			LeafNode = 1 << 0
		}

		sealed class NodeBlock
		{
			// Node (S0, k1, S1, ..., kn, Sn)
			// where n = Order - 1
			// S0 = FirstChildNodeBlockId
			// (kn, Sn) = new Item(this, n - 1)

			public const int NodeHeaderMagic1Offset = 0;
			public const int NodeHeaderMagic2Offset = 1;
			public const byte NodeHeaderMagic1 = 0x81;
			public const byte NodeHeaderMagic2 = 0xab;
			public const int NodeHeaderFlagsOffset = 2;
			public const int NodeHeaderFirstItemPointerOffset = 4;
			public const int NodeHeaderFirstNodePointerOffset = 6;
			public const int NodeHeaderSize = 16;

			public const int InvalidIndex = 0xffff;

			public readonly BTree Tree;

			public NodeBlock Parent;
			public int IndexInParent;

			public readonly byte[] Bytes;
			public readonly InternalUtils.BitConverter BitCvt;
			public readonly Bitmap FreeIndexMap;
			public bool Dirty;

			public struct Item
			{
				readonly public NodeBlock Node;

				public readonly int Index;
				public readonly int Offset;

				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				public Item(NodeBlock node, int index)
				{
					if (index < 0 || index >= node.Tree.order - 1) {
						throw new ArgumentOutOfRangeException("index");
					}

					Node = node;
					Index = index;
					Offset = NodeHeaderSize + Node.ItemSize * Index;
				}

				public int NextIndex
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return Node.BitCvt.GetUInt16 (Offset); }
					set { 
						Node.BitCvt.Set (Offset, checked((ushort)value));
						Node.Dirty = true;
					}
				}
				public long LinkedBlockId
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return Node.BitCvt.GetInt64 (Offset + 2); }
					set { 
						Node.BitCvt.Set (Offset + 2, value); 
						Node.Dirty = true;
					}
				}
				// byte offset in the node block buffer to the actual key
				public int KeyOffset
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return Offset + 10 + Node.Tree.keyLengthSize; }
				}
				public int ActualKeyLength
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return checked((int)Node.BitCvt.GetVariant (Offset + 10, Node.Tree.keyLengthSize)); }
					private set { 
						Node.BitCvt.SetVariant (Offset + 10, Node.Tree.keyLengthSize, checked((ulong)value)); 
						Node.Dirty = true;
					}
				}
				public int ValueLength
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return checked((int)Node.BitCvt.GetVariant (KeyOffset + ActualKeyLength, Node.Tree.keyLengthSize)); }
					set { 
						Node.BitCvt.SetVariant (KeyOffset + ActualKeyLength, Node.Tree.keyLengthSize, checked((ulong)value)); 
						Node.Dirty = true;
					}
				}
				public int ValueOffset
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get { return KeyOffset + ActualKeyLength + Node.Tree.keyLengthSize; }
				}
				public int MaxValueLength
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get {
						return Node.ItemSize - (ValueOffset - Offset);
					}
				}
				public long OverflowPageBlockId
				{
					[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
					get {
						if (ValueLength != 0) {
							throw new InvalidOperationException ("Item doesn't have an overflow page block id.");
						}
						return Node.BitCvt.GetInt64 (ValueOffset);
					}
					set {
						if (ValueLength != 0) {
							throw new InvalidOperationException ("Item doesn't have an overflow page block id.");
						}
						Node.BitCvt.Set (ValueOffset, value);
						Node.Dirty = true;
					}
				}
				public byte[] GetKey()
				{
					var bytes = Node.Bytes;
					var offset = KeyOffset;
					var len = ActualKeyLength;
					var ret = new byte[len];
					Buffer.BlockCopy (bytes, offset, ret, 0, len);
					return ret;
				}
				public byte[] GetInTreeValue()
				{
					var bytes = Node.Bytes;
					var offset = ValueOffset;
					var len = ValueLength;
					if (len == 0 && OverflowPageBlockId != 0) {
						throw new InvalidOperationException ("The value of the item is not stored in the tree.");
					}
					var ret = new byte[len];
					Buffer.BlockCopy (bytes, offset, ret, 0, len);
					return ret;
				}
				public void SetKey(byte[] key, int start, int length)
				{
					if (length > Node.Tree.header.MaximumEffectiveKeyLength) {
						throw new ArgumentOutOfRangeException ("length", "Key too long.");
					}
					ActualKeyLength = length;
					Buffer.BlockCopy (key, start, Node.Bytes, KeyOffset, length);
					ValueLength = 0;
					OverflowPageBlockId = 0;
				}
				public void CopyFrom(Item item)
				{
					Buffer.BlockCopy (item.Node.Bytes, item.Offset,
						Node.Bytes, Offset, Node.ItemSize);
				}
			}

			public long? BlockId { get; private set; }

			public NodeBlock(BTree tree)
			{
				this.Tree = tree;

				Bytes = new byte[Storage.BlockSize];
				BitCvt = new InternalUtils.BitConverter(Bytes);

				FreeIndexMap = new Bitmap(tree.order - 1);
			}

			StellaDB.IO.IBlockStorage Storage
			{
				get { return Tree.storage; }
			}

			int ItemSize
			{
				get { 
					return Tree.itemSize;
				}
			}

			public NodeFlags Flags
			{
				get {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					return (NodeFlags)BitCvt.GetUInt16 (NodeHeaderFlagsOffset);
				}
				set {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					BitCvt.Set (NodeHeaderFlagsOffset, (ushort)value);
					Dirty = true;
				}
			}

			int FirstItemIndex
			{
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					return (int)BitCvt.GetUInt16 (NodeHeaderFirstItemPointerOffset);
				}
				set {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					BitCvt.Set (NodeHeaderFirstItemPointerOffset, (ushort)value);
					Dirty = true;
				}
			}
			long FirstChildNodeBlockId
			{
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					return BitCvt.GetInt64 (NodeHeaderFirstNodePointerOffset);
				}
				set {
					if (BlockId == null) {
						throw new InvalidOperationException ("Node is not loaded.");
					}
					BitCvt.Set (NodeHeaderFirstNodePointerOffset, value);
					Dirty = true;
				}
			}

			int LastItemIndex
			{
				get {
					int i = FirstItemIndex, last = i;
					while (i != InvalidIndex) {
						last = i;
						i = GetItem (i).NextIndex;
					}
					return last;
				}
			}

			int NumItems
			{
				get {
					return FreeIndexMap.NumZeros;
				}
			}

			long PreviousSiblingBlockId
			{
				get {
					if (Parent == null || IndexInParent == InvalidIndex) {
						return 0;
					}
					int prev = Parent.GetPreviousIndex (IndexInParent);
					if (prev == InvalidIndex) {
						return Parent.FirstChildNodeBlockId;
					} else {
						return Parent.GetItem (prev).LinkedBlockId;
					}
				}
			}
			long NextSiblingBlockId
			{
				get {
					if (Parent == null) {
						return 0;
					}
					int next = IndexInParent == InvalidIndex ? Parent.FirstItemIndex :
						Parent.GetNextIndex (IndexInParent);
					if (next == InvalidIndex) {
						return 0;
					} else {
						return Parent.GetItem (next).LinkedBlockId;
					}
				}
			}

			void Load(long blockId, NodeBlock parent, int indexInParent)
			{
				if (BlockId == blockId) {
					return;
				}
				Unload ();

				Storage.ReadBlock ((long)blockId, Bytes, 0);
				Dirty = false;
				Parent = parent;
				IndexInParent = indexInParent;

				if (Bytes[NodeHeaderMagic1Offset] != NodeHeaderMagic1 ||
					Bytes[NodeHeaderMagic2Offset] != NodeHeaderMagic2) {
					throw new InvalidMagicNumberException ();
				}

				// build free index map
				FreeIndexMap.FillOne ();
				BlockId = blockId;

				for (int index = FirstItemIndex; index != InvalidIndex;) {
					FreeIndexMap [index] = false;
					index = new Item (this, index).NextIndex;
				}

			}

			void InitializeNewBlock(long blockId, NodeBlock parent, int indexInParent)
			{
				Unload ();

				BlockId = blockId;
				Parent = parent;
				IndexInParent = indexInParent;

				Bytes [NodeHeaderMagic1Offset] = NodeHeaderMagic1;
				Bytes [NodeHeaderMagic2Offset] = NodeHeaderMagic2;

				Flags = NodeFlags.LeafNode;

				FreeIndexMap.FillOne ();

				FirstItemIndex = 0xffff;
				FirstChildNodeBlockId = 0;

				Dirty = true;
			}

			public void InitializeEmptyRoot(long blockId)
			{
				InitializeNewBlock (blockId, null, InvalidIndex);
			}

			public void LoadRoot(long blockId)
			{
				Load (blockId, null, InvalidIndex);
			}

			// Only used for reading/writing existing elements.
			// Some properties of the node loaded with this method might return
			// an invalid value.
			public void LoadForAccessingItem(long blockId)
			{
				Load (blockId, null, InvalidIndex);
			}

			public void Unload()
			{
				Write ();
				Parent = null;
				BlockId = null;
			}

			/// <summary>
			/// Result of FindKey, which can be one of the following state:
			/// <list type="number"> 
			/// <item>ExactMatch = true, which means the specified key equals to NextIndex.</item>
			/// <item>PrevIndex = InvalidIndex, NextIndex = InvalidIndex which means
			/// 	the node is empty.</item>
			/// <item>PrevIndex = InvalidIndex, NextIndex = FirstItemIndex which means
			///     the specified key was less than any elements.</item>
			/// <item>NextIndex = InvalidIndex which means the specified key was greater than
			///     any elements.</item>
			/// <item>Otherwise, it means the specified key was greater than
			///     PrevIndex but less than NextIndex.</item>
			/// </list>
			/// </summary>
			public struct FindResult
			{
				public int PrevIndex;
				public int NextIndex;

				// true when key matches the NextIndex's one
				public bool ExactMatch;
			}

			public FindResult FindKey(byte[] key, int offset, int len)
			{
				// TODO: use binary search

				if (FirstItemIndex == InvalidIndex) {
					return new FindResult () {
						PrevIndex = InvalidIndex,
						NextIndex = InvalidIndex,
						ExactMatch = false
					};
				}

				// Build index map for binary search
				int[] indices = Tree.indices;
				int numItems = 0;
				int index = FirstItemIndex;
				while (index != InvalidIndex) {
					indices [numItems++] = index;
					index = new Item (this, index).NextIndex;
				}

				int start = 0, end = numItems;
				var comparer = Tree.comparer;

				while (end > start) {
					int mid = (start + end) >> 1;
					var cur = new Item (this, indices[mid]);
					int ret = comparer.Compare(key, offset, len, Bytes, cur.KeyOffset, cur.ActualKeyLength);
					if (ret > 0) {
						start = mid + 1;
					} else if (ret == 0) {
						return new FindResult () {
							PrevIndex = mid == 0 ? InvalidIndex : indices[mid - 1],
							NextIndex = indices[mid],
							ExactMatch = true
						};
					} else {
						end = mid;
					}
				}

				bool exactMatch = false;
				if (start != numItems) {
					var cur = new Item (this, indices[start]);
					int ret = comparer.Compare(key, offset, len, Bytes, cur.KeyOffset, cur.ActualKeyLength);
					if (ret == 0)
						exactMatch = true;
				}
				return new FindResult () {
					PrevIndex = start == 0 ? InvalidIndex : indices[start - 1],
					NextIndex = start == numItems ? InvalidIndex : indices[start],
					ExactMatch = exactMatch
				};

				/* Linear search O(N) for sufficiently large key size
				int index = FirstItemIndex;
				int lastIndex = InvalidIndex;
				while (index != InvalidIndex) {
					var cur = new Item (this, index);
					int ret = Tree.comparer.Compare(key, offset, len, Bytes, cur.KeyOffset, cur.ActualKeyLength);
					if (ret > 0) {
						lastIndex = index;
						index = cur.NextIndex;
					} else {
						return new FindResult () {
							PrevIndex = lastIndex,
							NextIndex = index,
							ExactMatch = ret == 0
						};
					}
				}
				return new FindResult () {
					PrevIndex = lastIndex,
					NextIndex = index,
					ExactMatch = false
				};*/
			}

			/// <summary>
			/// Moves the cursor until the node to which it point becomes the node where
			/// the specified key could appear.
			/// </summary>
			public static FindResult FindNode(NodeCursor cursor, byte[] key, int offset, int len)
			{
				if (cursor == null)
					throw new ArgumentNullException ("cursor");

				// do tree traversal
				FindResult result;
				cursor.ActiveLevel = 0;
				while (true) {
					var node = cursor.Top;
					result = node.FindKey (key, offset, len);
					if (result.ExactMatch) {
						return result;
					}

					long nextBlockId;
					if (result.PrevIndex == InvalidIndex) {
						nextBlockId = node.FirstChildNodeBlockId;
					} else {
						nextBlockId = new Item (node, result.PrevIndex).LinkedBlockId;
					}

					if (nextBlockId == 0) {
						return result;
					} else {
						EnterNextLevel (cursor, nextBlockId, result.PrevIndex);
					}
				}
			}

			public static void EnterNextLevel(NodeCursor cursor, long blockId, int indexInParent)
			{
				int i = cursor.ActiveLevel;
				++i;
				if (i >= cursor.Nodes.Count) {
					cursor.Nodes.Add (new NodeBlock (cursor.Top.Tree));
				}
				cursor.Nodes[i].Load (blockId, cursor.Nodes[i - 1], indexInParent);
				cursor.ActiveLevel = i;
			}

			/// <summary>
			/// Finds the node which contains the least item in the descendants of the current top node.
			/// </summary>
			/// <param name="cursor">Cursor.</param>
			/// <returns>The index of the least item. Cursor is moved to point its node.</returns>
			public static int FindLeastItem(NodeCursor cursor)
			{
				while (true) {
					var node = cursor.Top;
					if (node.FirstChildNodeBlockId == 0) {
						// Leaf.
						return node.FirstItemIndex;
					}
					EnterNextLevel (cursor, node.FirstChildNodeBlockId, InvalidIndex);
				}
			}

			/// <summary>
			/// Finds the node which contains the least item in the descendants of the current top node.
			/// </summary>
			/// <param name="cursor">Cursor.</param>
			/// <returns>The index of the least item. Cursor is moved to point its node.</returns>
			public static int FindGreatestItem(NodeCursor cursor)
			{
				while (true) {
					var node = cursor.Top;
					if (node.FirstChildNodeBlockId == 0) {
						// Leaf.
						return node.LastItemIndex;
					}

					var item = node.GetItem (node.LastItemIndex);
					EnterNextLevel (cursor, item.LinkedBlockId, item.Index);
				}
			}



			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public Item GetItem(int index)
			{
				return new Item(this, index);
			}

			/// <summary>
			/// Inserts the key. The node must be chosen with FindNode.
			/// </summary>
			public static void InsertKey
			(FindResult pos, NodeCursor cursor, byte[] key, int start, int length)
			{
				if (pos.ExactMatch) {
					throw new InvalidOperationException ("Attempted to insert an existing key.");
				}

				var tree = cursor.Top.Tree;
				tree.TreeStateAboutToBeUpdated();

				int level = cursor.ActiveLevel;

				// make temporal item.
				var tmpNode = tree.cursor.TemporalNode2;
				Item insertedItem;
				tmpNode.FreeIndexMap.FillOne ();
				{
					var idx = tmpNode.AllocateIndex (false);
					insertedItem = new Item(tmpNode, (int)idx);
					insertedItem.SetKey(key, start, length);
					insertedItem.NextIndex = InvalidIndex;
					insertedItem.LinkedBlockId = 0;
				}

				long insertedPrevBlockId = 0;

				while (level >= 0) {
					var node = cursor.Nodes [level];
					if (pos.PrevIndex == InvalidIndex) {
						pos.NextIndex = node.FirstItemIndex;
					} else {
						pos.NextIndex = new Item (node, pos.PrevIndex).NextIndex;
					}

					#if Validate
					if (pos.PrevIndex != InvalidIndex) {
					var p = node.GetItem (pos.PrevIndex);
					if (tree.comparer.Compare(insertedItem.Node.Bytes, insertedItem.KeyOffset, insertedItem.ActualKeyLength,
					p.Node.Bytes, p.KeyOffset, p.ActualKeyLength) < 0) {
					throw new InvalidOperationException ();
					}
					}
					if (pos.NextIndex != InvalidIndex) {
					var p = node.GetItem (pos.NextIndex);
					if (tree.comparer.Compare(insertedItem.Node.Bytes, insertedItem.KeyOffset, insertedItem.ActualKeyLength,
					p.Node.Bytes, p.KeyOffset, p.ActualKeyLength) > 0) {
					throw new InvalidOperationException ();
					}
					}

					#endif

					var lowerNode = level < cursor.ActiveLevel ? cursor.Nodes [level + 1] : null;

					node.Validate ();

					// try to allocate new index
					int? freeIndex = node.AllocateIndex(false);
					if (freeIndex != null) {
						node.Validate ();

						var item = new Item(node, (int)freeIndex);
						item.CopyFrom (insertedItem);
						item.NextIndex = pos.NextIndex;
						if (pos.PrevIndex == InvalidIndex) {
							node.FirstItemIndex = (int)freeIndex;
							if (node.FirstChildNodeBlockId != item.LinkedBlockId) {
								throw new InvalidOperationException ();
							}
							node.FirstChildNodeBlockId = insertedPrevBlockId;
							if (lowerNode != null &&
								(long)lowerNode.BlockId == node.FirstChildNodeBlockId) {
								lowerNode.IndexInParent = InvalidIndex;
							}
						} else {
							var itm = new Item (node, pos.PrevIndex);
							itm.NextIndex = (int)freeIndex;
							if (itm.LinkedBlockId != item.LinkedBlockId) {
								throw new InvalidOperationException ();
							}
							itm.LinkedBlockId = insertedPrevBlockId;
							if (lowerNode != null &&
								(long)lowerNode.BlockId == itm.LinkedBlockId) {
								lowerNode.IndexInParent = itm.Index;
							}
						}
						if (lowerNode != null &&
							(long)lowerNode.BlockId == item.LinkedBlockId) {
							lowerNode.IndexInParent = item.Index;
						}

						node.Write ();
						node.Validate ();
						return;
					}


					if (pos.PrevIndex == InvalidIndex) {
						if (node.FirstChildNodeBlockId != insertedItem.LinkedBlockId) {
							throw new InvalidOperationException ();
						}
						node.FirstChildNodeBlockId = insertedPrevBlockId;
						if (lowerNode != null &&
							(long)lowerNode.BlockId == insertedPrevBlockId) {
							lowerNode.IndexInParent = InvalidIndex;
						}
					} else {
						var itm = new Item (node, pos.PrevIndex);
						if (itm.LinkedBlockId != insertedItem.LinkedBlockId) {
							throw new InvalidOperationException ();
						}
						itm.LinkedBlockId = insertedPrevBlockId;
						if (lowerNode != null &&
							(long)lowerNode.BlockId == insertedPrevBlockId) {
							lowerNode.IndexInParent = pos.PrevIndex;
						}
					}

					// Overflow. Need to split the node.
					// Allocate another node and move the Floor [(Order + 1) / 2] items to it.
					int index = node.FirstItemIndex;
					int movedCount = (tree.order + 1) / 2;
					var newNode = tree.cursor.GetTemporaryBlock();
					newNode.InitializeNewBlock (tree.db.Freemap.AllocateBlock (), node.Parent, node.IndexInParent);
					newNode.FirstChildNodeBlockId = node.FirstChildNodeBlockId;

					int? nextIndex = null;
					bool haveInsertedNewItem = false;

					if (pos.PrevIndex == InvalidIndex) {
						var idx = newNode.AllocateIndex(false);
						if (idx == null) {
							throw new InvalidOperationException("Failed to allocate an index in a fresh block.");
						}

						var item = new Item(newNode, (int)idx);
						item.CopyFrom (insertedItem);
						if (lowerNode != null &&
							(long)lowerNode.BlockId == item.LinkedBlockId) {
							lowerNode.IndexInParent = item.Index;
						}

						nextIndex = idx;
						newNode.FirstItemIndex = (int)idx;
						haveInsertedNewItem = true;
						--movedCount;
					}

					while (movedCount > 0) {
						if (index == InvalidIndex) {
							throw new InvalidOperationException();
						}

						var idx = newNode.AllocateIndex(false);
						if (idx == null) {
							throw new InvalidOperationException("Failed to allocate an index in a fresh block.");
						}

						var item = new Item(newNode, (int)idx);
						var oldItem = new Item(node, index);
						item.CopyFrom (oldItem);
						if (lowerNode != null &&
							(long)lowerNode.BlockId == item.LinkedBlockId) {
							lowerNode.IndexInParent = item.Index;
						}
						if (nextIndex != null) {
							new Item(newNode, (int)nextIndex).NextIndex = (int)idx;
						} else {
							newNode.FirstItemIndex = (int)idx;
						}
						nextIndex = idx;
						--movedCount;

						if (pos.PrevIndex == index) {
							idx = newNode.AllocateIndex(false);
							if (idx == null) {
								throw new InvalidOperationException("Failed to allocate an index in a fresh block.");
							}

							item = new Item(newNode, (int)idx);
							item.CopyFrom (insertedItem);
							if (lowerNode != null &&
								(long)lowerNode.BlockId == item.LinkedBlockId) {
								lowerNode.IndexInParent = item.Index;
							}

							new Item(newNode, (int)nextIndex).NextIndex = (int)idx;
							nextIndex = idx;
							haveInsertedNewItem = true;
							--movedCount;
						}

						// deallocate
						node.FreeIndexMap [index] = true;

						// move next...
						index = oldItem.NextIndex;
					}
					node.FirstItemIndex = index;

					// move the left node's last child to the right node.
					// The left node's last key is a median and will be removed later...
					node.FirstChildNodeBlockId = new Item (newNode, (int)nextIndex).LinkedBlockId;
					if (lowerNode != null &&
						(long)lowerNode.BlockId == node.FirstChildNodeBlockId) {
						lowerNode.IndexInParent = InvalidIndex;
					}

					node.Validate ();

					if (!haveInsertedNewItem) {
						// newly inserted item should come in the right node.
						freeIndex = node.AllocateIndex(false);
						var item = new Item(node, (int)freeIndex);
						item.CopyFrom (insertedItem);
						item.NextIndex = pos.NextIndex;
						if (pos.PrevIndex == InvalidIndex) {
							node.FirstItemIndex = (int)freeIndex;
						} else {
							new Item (node, pos.PrevIndex).NextIndex = (int)freeIndex;
						}
						if (lowerNode != null &&
							(long)lowerNode.BlockId == item.LinkedBlockId) {
							lowerNode.IndexInParent = item.Index;
						}
					} else {
						// Newly inserted item is in newNode (left node).
						// Update the cursor to make the access to the inserted item
						// faster.
						tree.cursor.SwapBlockWithTemporalBlock (level);
						if (level < tree.cursor.ActiveLevel) {
							tree.cursor.Nodes [level + 1].Parent = tree.cursor.Nodes [level];
						}
						// now cursor follows the left node.
					}

					// now half of the items are moved to the new node, but
					// it is not connected to any nodes yet.
					// take the last moved element (median).
					new Item(newNode, newNode.GetPreviousIndex((int)nextIndex)).NextIndex = InvalidIndex;
					newNode.Validate ();

					insertedItem.CopyFrom (new Item (newNode, (int)nextIndex));
					newNode.FreeIndexMap [(int)nextIndex] = true;

					insertedItem.LinkedBlockId = (long)node.BlockId;
					insertedPrevBlockId = (long)newNode.BlockId;

					// now we can write back the newly created node.
					// (note that this might not be newNode)
					node.Write ();
					newNode.Write ();

					node.Validate ();
					newNode.Validate ();

					pos.PrevIndex = node.IndexInParent;
					if (node.Parent != null) {
						if (node.Parent.GetBlockIdForIndex(pos.PrevIndex) !=
							(long)node.BlockId) {
							throw new InvalidOperationException ();
						}
					}
					tree.cursor.GetTemporaryBlock ().Unload ();
					--level;
				}

				// Root has overflown. Need to create an new root.
				{
					var newNode = tree.cursor.InsertNewRoot ();
					newNode.InitializeEmptyRoot (tree.db.Freemap.AllocateBlock ());

					var idx = newNode.AllocateIndex (false);
					var item = new Item(newNode, (int)idx);
					item.CopyFrom (insertedItem);
					item.NextIndex = InvalidIndex;
					newNode.FirstItemIndex = (int)idx;

					newNode.FirstChildNodeBlockId = insertedPrevBlockId;
					tree.cursor.Nodes [1].Parent = newNode;
					tree.cursor.Nodes [1].IndexInParent = 
						(insertedPrevBlockId == (long)tree.cursor.Nodes [1].BlockId) ? InvalidIndex : 0;

					tree.header.RootNodeBlockId = (long)newNode.BlockId;
					tree.header.Write ();
				}
			}

			/// <summary>
			/// Deletes the item <c><paramref name="cursor"/>.Top.GetItem(<paramref name="index"/>)</c>.
			/// </summary>
			/// <param name="index">Index of the item being deleted.</param>
			/// <param name="cursor">Cursor.</param>
			public static void DeleteKey
			(int index, NodeCursor cursor)
			{
				cursor.Top.Tree.TreeStateAboutToBeUpdated();
				{
					var item = cursor.Top.GetItem (index);
					if (item.LinkedBlockId != 0) {
						// Internal node.
						// We are removing the separation value, so we need a
						// substitution.
						EnterNextLevel (cursor, item.LinkedBlockId, item.Index);
						var idx = FindLeastItem (cursor);
						var subst = cursor.Top.GetItem (idx);
						var oldNextIndex = item.NextIndex;
						var oldLinkedBlock = item.LinkedBlockId;
						item.CopyFrom (subst);
						item.NextIndex = oldNextIndex;
						item.LinkedBlockId = oldLinkedBlock;
						cursor.Top.FreeIndexMap [subst.Index] = true;
						cursor.Top.FirstItemIndex = subst.NextIndex;
						//item.Node.Validate ();
					} else {
						// Leaf node.
						// Simply remove the item.
						var node = cursor.Top;
						var prev = node.GetPreviousIndex (index);
						if (prev == InvalidIndex) {
							node.FirstItemIndex = item.NextIndex;
						} else {
							new Item (node, prev).NextIndex = item.NextIndex;
						}
						node.FreeIndexMap [item.Index] = true;
						item.Node.Validate ();
					}

				}

				var tmpNode = cursor.GetTemporaryBlock();
				var tmpNode2 = cursor.TemporalNode2;

				// Rebalancing.
				while (cursor.ActiveLevel > 0) { // Root doesn't have to be rebalanced in a usual way
					var node = cursor.Top;
					var numItems = node.NumItems;
					if (numItems >= node.Tree.minimumNumItems) {
						// No need to rebalance
						return;
					}

					// Index in the parent
					int parentIndex = node.IndexInParent;
					int nextIndex = node.Parent.GetNextIndex(parentIndex);	
					int prevIndex = parentIndex != InvalidIndex ?
						node.Parent.GetPreviousIndex(parentIndex) : InvalidIndex;	

					NodeBlock prevSib = null;
					NodeBlock nextSib = null;

					// This node has too few items. Need to rebalance.
					// Check siblings if we can rotate the tree.
					{
						var nextSibId = nextIndex != InvalidIndex ?
							node.Parent.GetBlockIdForIndex(nextIndex) : 0;
						if (nextSibId != 0) {
							tmpNode.Load (nextSibId, node.Parent, nextIndex);
							nextSib = tmpNode;
							if (nextSib.NumItems > node.Tree.minimumNumItems) {
								// The sibling has enough items. Rotate left.
								var newIndex = node.AllocateIndexNeverFail (false);
								var newItem = node.GetItem (newIndex);
								var sepItem = node.Parent.GetItem (nextIndex);
								var oldItem = nextSib.GetItem (nextSib.FirstItemIndex);

								// Add a new item to the left
								newItem.CopyFrom (sepItem);
								new Item (node, node.LastItemIndex).NextIndex = newItem.Index;
								newItem.NextIndex = InvalidIndex;
								newItem.LinkedBlockId = nextSib.FirstChildNodeBlockId;

								// Overwrite the separator with the old item
								var oldNext = sepItem.NextIndex;
								sepItem.CopyFrom (oldItem);
								sepItem.LinkedBlockId = nextSibId;
								sepItem.NextIndex = oldNext;

								// Remove the old item
								nextSib.FirstChildNodeBlockId = oldItem.LinkedBlockId;
								nextSib.FirstItemIndex = oldItem.NextIndex;
								nextSib.FreeIndexMap [oldItem.Index] = true;

								nextSib.Validate ();
								nextSib.Unload ();

								//node.Validate ();
								return;
							}
						}
					}
					if (parentIndex != InvalidIndex) {
						var prevSibId = node.Parent.GetBlockIdForIndex(prevIndex);
						if (prevSibId != 0) {
							tmpNode2.Load (prevSibId, node.Parent, prevIndex);
							prevSib = tmpNode2;
							if (prevSib.NumItems > node.Tree.minimumNumItems) {
								// The sibling has enough items. Rotate right.
								var newIndex = node.AllocateIndexNeverFail (false);
								var newItem = node.GetItem (newIndex);
								var sepItem = node.Parent.GetItem (parentIndex);
								var oldItem = prevSib.GetItem (prevSib.LastItemIndex);

								// Add a new item to the right
								newItem.CopyFrom (sepItem);
								newItem.NextIndex = node.FirstItemIndex;
								node.FirstItemIndex = newItem.Index;
								newItem.LinkedBlockId = node.FirstChildNodeBlockId;
								node.FirstChildNodeBlockId = oldItem.LinkedBlockId;

								// Overwrite the separator with the old item
								var oldNext = sepItem.NextIndex;
								sepItem.CopyFrom (oldItem);
								sepItem.LinkedBlockId = (long)node.BlockId;
								sepItem.NextIndex = oldNext;

								// Remove the old item
								new Item (prevSib, prevSib.GetPreviousIndex (oldItem.Index)).NextIndex = InvalidIndex;
								prevSib.FreeIndexMap [oldItem.Index] = true;

								prevSib.Validate ();
								prevSib.Unload ();

								if (nextSib != null) {
									nextSib.Unload ();
								}
								//node.Validate ();
								return;
							}
						}
					}

					// Both siblings didn't have enough items to rotate.
					// Try merging with siblings.
					if (nextSib != null) {

						// Copy separator.
						int lastIndex = node.LastItemIndex;
						{
							var sepItem = node.Parent.GetItem (nextIndex);
							var newIndex = node.AllocateIndexNeverFail(false);
							var newItem = node.GetItem(newIndex);
							newItem.CopyFrom(sepItem);
							newItem.NextIndex = InvalidIndex;
							newItem.LinkedBlockId = nextSib.FirstChildNodeBlockId;
							if (lastIndex == InvalidIndex) {
								node.FirstItemIndex = newItem.Index;
							} else {
								new Item(node, lastIndex).NextIndex = newItem.Index;
							}
							lastIndex = newItem.Index;
						}

						// Copy items of the next sibling node to the current one.
						int curIndex = nextSib.FirstItemIndex;
						while (curIndex != InvalidIndex) {
							var item = nextSib.GetItem(curIndex);

							var newIndex = node.AllocateIndexNeverFail(false);
							var newItem = node.GetItem(newIndex);
							newItem.CopyFrom(item);
							new Item(node, lastIndex).NextIndex = newItem.Index;
							lastIndex = newItem.Index;

							curIndex = item.NextIndex;
						}

						new Item (node, lastIndex).NextIndex = InvalidIndex;

						// Remove the separator and the right node.
						if (parentIndex == InvalidIndex) {
							node.Parent.FirstItemIndex = 
								new Item (node.Parent, nextIndex).NextIndex;
						} else {
							new Item (node.Parent, parentIndex).NextIndex =
								new Item (node.Parent, nextIndex).NextIndex;
						}
						node.Parent.FreeIndexMap [nextIndex] = true;
						nextSib.Deallocate ();
					} else if (prevSib != null) {

						// Copy separator.
						{
							var sepItem = node.Parent.GetItem (parentIndex);
							var newIndex = node.AllocateIndexNeverFail(false);
							var newItem = node.GetItem(newIndex);
							newItem.CopyFrom(sepItem);
							newItem.NextIndex = node.FirstItemIndex;
							newItem.LinkedBlockId = node.FirstChildNodeBlockId;
							node.FirstItemIndex = newItem.Index;
						}

						int originalItemsFirstIndex = node.FirstItemIndex;

						// Copy items of the prev sibling to the current one.
						int curIndex = prevSib.FirstItemIndex;
						int lastIndex = InvalidIndex;
						while (curIndex != InvalidIndex) {
							var item = prevSib.GetItem(curIndex);

							var newIndex = node.AllocateIndexNeverFail(false);
							var newItem = node.GetItem(newIndex);
							newItem.CopyFrom(item);
							if (lastIndex == InvalidIndex) {
								node.FirstItemIndex = newItem.Index;
							} else {
								new Item(node, lastIndex).NextIndex = newItem.Index;
							}
							lastIndex = newItem.Index;

							curIndex = item.NextIndex;
						}

						new Item (node, lastIndex).NextIndex = originalItemsFirstIndex;
						node.FirstChildNodeBlockId = prevSib.FirstChildNodeBlockId;

						// Remove the separator and the left node.
						if (parentIndex == node.Parent.FirstItemIndex) {
							node.Parent.FirstItemIndex = nextIndex;
							node.Parent.FirstChildNodeBlockId = (long)node.BlockId;
							node.Parent.FreeIndexMap [parentIndex] = true;
						} else {
							new Item (node.Parent, prevIndex).LinkedBlockId = (long)node.BlockId;
							new Item (node.Parent, prevIndex).NextIndex =
								new Item (node.Parent, parentIndex).NextIndex;
							node.Parent.FreeIndexMap [parentIndex] = true;
						}
						prevSib.Deallocate ();
					} else {
						throw new InvalidOperationException("No siblings found.");
					}

					node.Validate ();

					// Continue rebalancing ancestors.
					node.Write ();
					--cursor.ActiveLevel;

					if (prevSib != null)
						prevSib.Unload ();
					if (nextSib != null)
						nextSib.Unload ();
				}

				// Rebalance the root.
				while (cursor.Nodes[0].NumItems == 0){
					cursor.ActiveLevel = 0;

					var node = cursor.Top;
					var newRoot = node.FirstChildNodeBlockId;
					if (newRoot == 0) {
						// empty tree
						break;
					}

					// Root only has one child (zero keys).
					// Make it root.
					node.Deallocate ();
					EnterNextLevel (cursor, newRoot, InvalidIndex);
					cursor.RemoveRoot ();
					cursor.Top.Parent = null;
					cursor.Top.Tree.header.RootNodeBlockId = newRoot;
					cursor.Top.Tree.header.Write ();
				}
			}

			public static int FindPreviousKey(NodeCursor cursor, int index)
			{
				if (index == InvalidIndex) {
					throw new InvalidOperationException ("Invalid index given.");
				}

				var node = cursor.Top;
				var prev = node.GetPreviousIndex (index);
				if (prev == InvalidIndex) {
					if (node.FirstChildNodeBlockId != 0) {
						EnterNextLevel (cursor, node.FirstChildNodeBlockId, InvalidIndex);
						return FindGreatestItem (cursor);
					} else {
						while (true) {
							if (cursor.ActiveLevel == 0) {
								return InvalidIndex;
							} else {
								index = cursor.Top.IndexInParent;
								--cursor.ActiveLevel;
								if (index != InvalidIndex) {
									return index;
								}
							}
						}
					}
				} else {
					var item = node.GetItem (prev);
					if (item.LinkedBlockId != 0) {
						EnterNextLevel (cursor, item.LinkedBlockId, item.Index);
						return FindGreatestItem (cursor);
					} else {
						return item.Index;
					}
				}
			}

			public static int FindNextKey(NodeCursor cursor, int index)
			{
				if (index == InvalidIndex) {
					throw new InvalidOperationException ("Invalid index given.");
				}

				var node = cursor.Top;
				var item = node.GetItem (index);
				if (item.LinkedBlockId != 0) {
					EnterNextLevel (cursor, item.LinkedBlockId, item.Index);
					return FindLeastItem (cursor);
				} else {
					if (item.NextIndex == InvalidIndex) {
						while (true) {
							if (cursor.ActiveLevel == 0) {
								return InvalidIndex;
							} else {
								index = cursor.Top.IndexInParent;
								--cursor.ActiveLevel;
								if (index == InvalidIndex) {
									return cursor.Top.FirstItemIndex;
								}
								index = cursor.Top.GetNextIndex (index);
								if (index != InvalidIndex) {
									return index;
								}
							}
						}
					} else {
						return item.NextIndex;
					}
				}
			}

			public static void DropTree(NodeCursor cursor)
			{
				var tree = cursor.Top.Tree;
				cursor.ActiveLevel = 0;
				while (cursor.ActiveLevel >= 0) {
					var node = cursor.Top;
					if (node.FirstChildNodeBlockId == 0) {
						// Leaf node. Cleanup all items.
						var idx = node.FirstItemIndex;
						while (idx != InvalidIndex) {
							var item = new Item (node, idx);
							var next = item.NextIndex;
							tree.CleanupCurrentEntry (idx);
							idx = next;
						}

						// Destroy.
						node.Deallocate ();
						--cursor.ActiveLevel;
					} else {
						// Internal node. Shift (popFront) the first child.
						long firstChild = node.FirstChildNodeBlockId;
						if (node.FirstItemIndex != InvalidIndex) {
							var item = new Item (node, node.FirstItemIndex);
							node.FirstChildNodeBlockId = item.LinkedBlockId;
							node.FirstItemIndex = item.NextIndex;
						} else {
							node.FirstChildNodeBlockId = 0;
						}
						EnterNextLevel (cursor, firstChild, InvalidIndex);
					}
				}

				// Now, tree are completely deallocated.
				cursor.ActiveLevel = 0;
			}

			int AllocateIndexNeverFail(bool temporal)
			{
				var index = AllocateIndex (temporal);
				if (index == null) {
					throw new DataInconsistencyException ("Failed to allocate an item index.");
				}
				return (int)index;
			}

			int? AllocateIndex(bool temporal)
			{
				int? i = FreeIndexMap.FindFirstOne ();

				// Bitmap.FindOne is aggressive
				if (i != null && i > Tree.order - 2) {
					return null;
				}

				if (i != null && !temporal) {
					FreeIndexMap [(int)i] = false;
				}
				return i;
			}

			int LinearizeIndex(int index)
			{
				int i = 0;
				int ind = FirstItemIndex;
				while (ind != InvalidIndex) {
					if (index == ind) {
						return i;
					}
					var cur = new Item (this, ind);
					ind = cur.NextIndex;
					++i;
				}
				throw new InvalidOperationException ("Specified index was not found in the node.");
			}

			int GetPreviousIndex(int nextIndex)
			{
				int index = FirstItemIndex;
				if (nextIndex == InvalidIndex) {
					throw new ArgumentException ("Cannot get the previous index of the front pointer.");
				}
				if (index == nextIndex) {
					return InvalidIndex;
				}
				while (index != InvalidIndex) {
					var cur = new Item (this, index);
					var n = cur.NextIndex;
					if (n == nextIndex) {
						return index;
					}
					index = n;
				}
				return InvalidIndex;
			}

			int GetNextIndex(int prevIndex)
			{
				if (prevIndex == InvalidIndex) {
					return FirstItemIndex;
				}
				return new Item (this, prevIndex).NextIndex;
			}

			long GetBlockIdForIndex(int index)
			{
				if (index == InvalidIndex) {
					return FirstChildNodeBlockId;
				} else {
					return new Item (this, index).LinkedBlockId;
				}
			}
			void SetBlockIdForIndex(int index, long blockId)
			{
				if (index == InvalidIndex) {
					FirstChildNodeBlockId = blockId;
				} else {
					new Item (this, index).LinkedBlockId = blockId;
				}
			}

			public void Deallocate()
			{
				if (BlockId == null) {
					return;
				}
				Tree.db.Freemap.DeallocateBlock ((long)BlockId);
				BlockId = null;
			}

			public void Write(int level = 0)
			{
				if (Parent != null) {
					if (level > 100) {
						throw new InvalidOperationException ();
					}
					Parent.Write (level + 1);
				}

				if (!Dirty || BlockId == null) {
					return;
				}

				Storage.WriteBlock ((long)BlockId, Bytes, 0);
				Dirty = false;
			}

			private void WriteIndent(TextWriter w, int level)
			{
				for(int i = 0; i < level; ++i){
					w.Write ("|  ");
				}
			}

			public void Validate(byte[] key1, byte[] key2, HashSet<long> blockIds)
			{
				#if Validate
				if (!Tree.ValidationEnabled) {
					return;
				}
				Tree.Flush();

				if (blockIds.Contains((long)BlockId)) {
					// Duplicate block ID
					throw new InvalidOperationException();
				}
				blockIds.Add((long)BlockId);

				/*if (Parent != null) {
					if (IndexInParent == InvalidIndex) {
						if (Parent.FirstChildNodeBlockId != (long)BlockId) {
							throw new InvalidOperationException();
						}
					} else {
						if (Parent.GetItem(IndexInParent).LinkedBlockId != (long)BlockId) {
							throw new InvalidOperationException();
						}
					}
				}*/

				int index = FirstItemIndex;
				byte[] last = key1;
				NodeBlock n = null;
				if (FirstChildNodeBlockId != 0) {
					n = new NodeBlock (Tree);
					n.Load (FirstChildNodeBlockId, this, InvalidIndex);
				}
				while (index != InvalidIndex) {
					var item = new Item (this, index);

					if (item.Index == item.NextIndex) {
						throw new InvalidOperationException();
					}

					byte[] b = item.GetKey ();
					if (Tree.comparer.Compare(b, 0, b.Length, key1, 0, key1.Length) <= 0) {
						throw new InvalidOperationException ();
					}
					if (Tree.comparer.Compare(b, 0, b.Length, key2, 0, key2.Length) >= 0) {
						throw new InvalidOperationException ();
					}

					if (n != null) {
						n.Validate (last, b, blockIds);
					}

					last = b;
					if (item.LinkedBlockId != 0) {
						n.Load (item.LinkedBlockId, this, InvalidIndex);
					}

					index = item.NextIndex;
				}
				if (n != null) {
					n.Validate (last, key2, blockIds);
				}
				#endif
			}

			// structure validation for debugging.
			// (This cannot be run in production code because it's VERY slow)
			public void Validate()
			{
				#if Validate
				byte[] b = new byte[255];
				for (int i = 0; i < b.Length; ++i)
				b [i] = 255;
				Validate (new byte[0], b, new HashSet<long>());

				// check level
				int level = 0;
				var e = this;
				while (e != null) {
					e = e.Parent;
					++level;
					if (level > 100) {
						throw new InvalidOperationException();
					}
				}

				// check cursor (this should be moved to NodeCursor or BTree)
				var cursor = Tree.cursor;
				for (int i = 0; i <= cursor.ActiveLevel; ++i) {
					var par = i == 0 ? null : cursor.Nodes[i - 1];
					if (cursor.Nodes[i].Parent != par) {
						throw new InvalidOperationException();
					}
				}

				#endif
			}

			

			public void Dump(TextWriter w, int level)
			{
				if (level > 16) {
					WriteIndent (w, level);
					w.WriteLine ("! TOO MANY LEVELS !");
					return;
				}
				WriteIndent (w, level);
				w.WriteLine ("[ {0} ]", BlockId);
				WriteIndent (w, level + 1);
				w.WriteLine ("< - >");
				if (FirstChildNodeBlockId != 0) {
					var n = new NodeBlock (Tree);
					n.Load (FirstChildNodeBlockId, this, InvalidIndex);
					n.Dump (w, level + 2);
				}

				int index = FirstItemIndex;
				while (index != InvalidIndex) {
					var item = new Item (this, index);

					byte[] b = item.GetKey ();
					var ls = from e in b
						select e.ToString ();
					string key = string.Join (", ", ls);
					if (key.Length > 30) {
						key = key.Substring (0, 30) + "...";
					}
					WriteIndent (w, level + 1);
					w.WriteLine ("< {0} >, key = {1}", index, key);


					if (item.LinkedBlockId != 0) {
						var n = new NodeBlock (Tree);
						n.Load (item.LinkedBlockId, this, index);
						n.Dump (w, level + 2);
					}

					index = item.NextIndex;
				}
			}

			public string StringDump
			{
				get {
					var tw = new StringWriter ();
					Dump (tw, 0);
					return tw.ToString ();
				}
			}
		}

		sealed class NodeCursor
		{
			public readonly List<NodeBlock> Nodes = new List<NodeBlock>();
			public int ActiveLevel = 0;
			public NodeBlock TemporalNode1;
			public readonly NodeBlock TemporalNode2;

			// FIXME: Cleanup BTree.NodeCursor

			public NodeCursor(NodeBlock root)
			{
				Nodes.Add(root);
				TemporalNode1 = new NodeBlock(root.Tree);
				TemporalNode2 = new NodeBlock(root.Tree);
			}

			public NodeBlock Top
			{
				get { return Nodes [ActiveLevel]; }
			}

			public NodeBlock GetTemporaryBlock()
			{
				return TemporalNode1;
			}
			public void SwapBlockWithTemporalBlock(int level)
			{
				var n = GetTemporaryBlock ();
				TemporalNode1 = Nodes [level];
				Nodes [level] = n;
			}
			public NodeBlock InsertNewRoot()
			{
				if (ActiveLevel < Nodes.Count - 1) {
					var n = Nodes[Nodes.Count - 1];
					Nodes.RemoveAt (Nodes.Count - 1);
					Nodes.Insert (0, n);
				} else {
					Nodes.Insert (0, new NodeBlock (Top.Tree));
				}
				++ActiveLevel;
				return Nodes [0];
			}
			public void RemoveRoot()
			{
				var r = Nodes [0];
				Nodes.RemoveAt (0);
				Nodes.Add (r);
				--ActiveLevel;
			}

		}

	}
}

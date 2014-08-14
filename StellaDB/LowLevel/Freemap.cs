//#define Validate

using System;
using System.Collections.Generic;


namespace Yavit.StellaDB.LowLevel
{
	class Freemap
	{

		readonly LowLevelDatabase db;
		readonly StellaDB.IO.IBlockStorage storage;

		readonly BufferPool blockPool;

		// When a node is completely empty, this value is used instead of actual block ID.
		// Note that block #0 is superblock, and can never be a node's block ID. 
		const long EmptyNodeBlockId = 0;

		// when a node is completely full, this value is OR-ed with the actual block ID.
		const long FullNodeBlockIdBit = unchecked((long)0x8000000000000000UL);

		// A leaf node contains free space bitmap.
		readonly int numBlocksInLeafNode;
		readonly int numBlocksInLeafNodeBits;

		// A internal node contains pointers to the nodes of the next level (depth).
		readonly int numChildrenInNode;
		readonly int numChildrenInNodeBits;

		// Number of internal nodes. Zero when only one leaf node can exist.
		int numLevels;

		#if Validate
		HashSet<long> onmemory = new HashSet<long>();
		#endif

		// Selection. Records the last used node and its parents.
		sealed class InternalNodeSelection
		{
			public readonly Freemap Freemap;
			public long BlockId = -1;
			public readonly long[] Children;
			public int SelectedIndex = -1;

			// true when bitmap must be written back
			public bool Dirty = false;

			public InternalNodeSelection(Freemap freemap)
			{
				this.Freemap = freemap;
				Children = new long[freemap.numChildrenInNode];
			}
			public void Load(long blockId) {
				Unload();
				this.BlockId = blockId;

				using (var buffer = Freemap.blockPool.CreateHandle()) {
					Freemap.storage.ReadBlock (blockId, buffer.Buffer, 0);
					Buffer.BlockCopy (buffer.Buffer, 0, Children, 0, buffer.Buffer.Length);
					Dirty = false;
				}
			}

			public void InitializeEmpty(long blockId) {
				Unload();
				this.BlockId = blockId;

				for (int i = 0; i < Children.Length; ++i) {
					Children [i] = EmptyNodeBlockId;
				}

				Dirty = true;
			}

			public bool IsFull() {
				for (int i = 0; i < Children.Length; ++i) {
					if ((Children [i] & FullNodeBlockIdBit) == 0) {
						return false;
					}
				}
				return true;
			}
			public bool IsEmpty() {
				for (int i = 0; i < Children.Length; ++i) {
					if (Children[i] != EmptyNodeBlockId) {
						return false;
					}
				}
				return true;
			}

			public void Flush() {
				if (Dirty) {
					using (var handle = Freemap.db.BufferPool.CreateHandle()) {
						var buffer = handle.Buffer;
						Buffer.BlockCopy (Children, 0, buffer, 0, buffer.Length);
						Freemap.storage.WriteBlock (BlockId, buffer, 0);
					}
				}
				Dirty = false;
			}

			public void Unload() {
				Flush();
				BlockId = -1;
			}
		}
		sealed class LeafNodeSelection
		{
			public readonly Freemap Freemap;
			public long BlockId = -1;

			// if bit is set, the corresponding block is free
			public readonly Bitmap Bitmap;

			// true when bitmap must be written back
			public bool Dirty = false;

			public LeafNodeSelection(Freemap freemap)
			{
				this.Freemap = freemap;
				Bitmap = new Bitmap(freemap.numBlocksInLeafNode);
			}

			public void Load(long blockId) {
				Unload();
				this.BlockId = blockId;

				using (var handle = Freemap.db.BufferPool.CreateHandle ()) {
					var buffer = handle.Buffer;
					Freemap.storage.ReadBlock (blockId, buffer, 0);
					Buffer.BlockCopy (buffer, 0, Bitmap.GetBuffer (), 0, buffer.Length);
				}

				Dirty = false;
				Bitmap.UpdateStatistics ();
			}


			public void Flush() {
				if (Dirty && BlockId != -1) {
					using (var handle = Freemap.db.BufferPool.CreateHandle ()) {
						var buffer = handle.Buffer;
						Buffer.BlockCopy (Bitmap.GetBuffer (), 0, buffer, 0, buffer.Length);
						Freemap.storage.WriteBlock (BlockId, buffer, 0);
					}
				}

				Dirty = false;
			}

			public void Unload() {
				Flush ();
				BlockId = -1;
			}
		}
		sealed class Selection
		{
			readonly Freemap freemap;

			public InternalNodeSelection[] Internals;
			public LeafNodeSelection Leaf;

			public Selection(Freemap freemap)
			{
				this.freemap = freemap;
				if (freemap == null) {
					throw new ArgumentNullException("freemap");
				}

				// make initial selection
				Internals = new InternalNodeSelection[freemap.numLevels];

				long blockId = freemap.Superblock.RootFreemapBlock;

				for (int i = 0; i < Internals.Length; ++i) {
					Internals[i] = new InternalNodeSelection(freemap);
					Internals[i].Load(blockId);

					long next = Internals[i].Children[0];
					if (next == EmptyNodeBlockId) {
						blockId = -1;
						break;
					} else {
						Internals[i].SelectedIndex = 0;
					}
					blockId = next & ~FullNodeBlockIdBit;
				}
				Leaf = new LeafNodeSelection(freemap);
				if (blockId != -1) {
					Leaf.Load(blockId);
				}
			}

			public void Flush() {
				foreach (var inode in Internals) {
					inode.Flush ();
				}
				Leaf.Flush ();
			}

			/// <summary>
			/// Returns the first block ID covered by the leaf node.
			/// </summary>
			public long GetLeafStartingBlockId() {
				return GetInternalNodeStartingBlockId (freemap.numLevels);
			}

			/// <summary>
			/// Returns the first block ID covered by the internal node.
			/// </summary>
			public long GetInternalNodeStartingBlockId(int level) {
				if (level < 0 || level > freemap.numLevels) {
					throw new ArgumentOutOfRangeException ("level");
				}
				long blockId = 0;
				for (int i = 0; i < level; ++i) {
					blockId <<= freemap.numChildrenInNodeBits;
					blockId += Internals [i].SelectedIndex;
				}
				for (int i = level; i < freemap.numLevels; ++i) {
					blockId <<= freemap.numChildrenInNodeBits;
				}
				blockId <<= freemap.numBlocksInLeafNodeBits;
				return blockId;
			}

			public void Select(int level, int index) {
				if (level < 0 || level >= freemap.numLevels) {
					throw new ArgumentOutOfRangeException ("level");
				}
				if (index < 0 || index >= freemap.numChildrenInNode) {
					throw new ArgumentOutOfRangeException ("index");
				}

				var inode = Internals[level];
				if (inode.SelectedIndex == index) {
					return;
				}

				inode.SelectedIndex = index;
				long next = inode.Children [index];
				if (level == Internals.Length - 1) {
					if (next == EmptyNodeBlockId) {
						Leaf.Unload ();
					} else {
						Leaf.Load (next & ~FullNodeBlockIdBit);
					}
				} else {
					var nextINode = Internals [level + 1];
					if (next == EmptyNodeBlockId) {
						nextINode.Unload ();
					} else {
						nextINode.Load (next & ~FullNodeBlockIdBit);
					}
				}
			}
		}
		Selection selection;

		Superblock Superblock
		{
			get { return db.Superblock; }
		}

		public Freemap (LowLevelDatabase db)
		{
			this.db = db;
			if (db == null) {
				throw new ArgumentNullException ("db");
			}

			storage = db.Storage;
			blockPool = db.BufferPool;

			// 8 bits/byte = 8 blocks/byte
			numBlocksInLeafNode = checked(storage.BlockSize * 8);
			numBlocksInLeafNodeBits = InternalUtils.GetBitWidth (numBlocksInLeafNode) - 1;

			// 8 bytes/blockId = 8 bytes/block
			numChildrenInNode = checked(storage.BlockSize / 8);
			numChildrenInNodeBits = InternalUtils.GetBitWidth (numChildrenInNode) - 1;

			numLevels = ComputeNumLevelsForDatabaseSize (Superblock.DatabaseSize);

			if (Superblock.RootFreemapBlock == 0) {
				// database is not yet initialized.
				// create root freemap at block #1.

				using (var handle = db.BufferPool.CreateHandle ()) {
					var buf = handle.Buffer;
					long[] buf2 = new long[storage.BlockSize / 8];
					long lastDatabaseSize = Superblock.DatabaseSize;

					do {
						// make internal nodes.
						// numLevels + 1 blocks will be used for initial freemap.
						if (numLevels + 2 > numBlocksInLeafNode) {
							// current implementation's restriction. Very rare.
							throw new NotSupportedException ();
						}

						// ensure database is big enough to store initial freemap.
						lastDatabaseSize = Superblock.DatabaseSize;
						Superblock.DatabaseSize = Math.Max (Superblock.DatabaseSize,
							numLevels + 2);
						storage.NumBlocks = Math.Max (storage.NumBlocks,
							Superblock.DatabaseSize);
						numLevels = ComputeNumLevelsForDatabaseSize (Superblock.DatabaseSize);

						// We have to recompute database size because changing database size
						// might change the number of levels.
					} while(lastDatabaseSize != Superblock.DatabaseSize);

					// Empty internal node.
					for (int i = 0; i < buf2.Length; ++i) {
						buf2 [i] = EmptyNodeBlockId;
					}
					Buffer.BlockCopy (buf2, 0, buf, 0, buf.Length);

					// for each internal node
					for (int i = 0; i < numLevels; ++i) {
						// link to the next level
						buf2 [0] = i + 2;
						Buffer.BlockCopy (buf2, 0, buf, 0, 8);
						storage.WriteBlock (i + 1, buf, 0);
					}

					// leaf node.
					for (int i = 0; i < buf.Length; ++i) {
						buf [i] = 0xff; // free
					}
					for (int i = 0; i < numLevels + 2; ++i) {
						buf [i >> 3] &= (byte)~(1 << (i & 7));
					}

					storage.WriteBlock (numLevels + 1, buf, 0);

					// now link superblock to the root freemap block.
					Superblock.RootFreemapBlock = 1;

					Superblock.NumAllocatedBlocks = numLevels + 2;
				}
			}

			// create initial selection.
			selection = new Selection (this);
		}

		int ComputeNumLevelsForDatabaseSize(long numBlocks) 
		{
			if (numBlocks <= 0) {
				numBlocks = 1;
			}

			int numBlocksBits = InternalUtils.GetBitWidth (numBlocks - 1);
			if (numBlocksBits <= numBlocksInLeafNodeBits) {
				return 0;
			}
			return (numBlocksBits - numBlocksInLeafNodeBits + numChildrenInNodeBits - 1) / numChildrenInNodeBits;
		}

		int ComputeIndexOfBlockAtLevel(long blockId, int level) 
		{
			if (level < 0 || level > numLevels) {
				throw new ArgumentOutOfRangeException ("level");
			}

			if (blockId < 0) {
				throw new ArgumentOutOfRangeException ("blockId");
			}

			ulong bId = checked((ulong)blockId);
			if (level == numLevels) {
				return (int)(bId & ((1UL << numBlocksInLeafNodeBits) - 1));
			}

			bId >>= (numLevels - 1 - level) * numChildrenInNodeBits + numBlocksInLeafNodeBits;

			if (level == 0 && bId >= (ulong)numChildrenInNode) {
				throw new ArgumentOutOfRangeException ("blockId");
			}

			return (int)(bId & ((1UL << numChildrenInNodeBits) - 1));
		}
			
		void MarkBlockAsUsed(long blockId) 
		{
			// TODO: make MarkBlockAsUsed fail-safe

			// When leaf node is not created yet,
			// internal nodes and leaf one must be allocated somewhere... 
			int numCreatedNodes = 0;
			long nextNodeBlockId = blockId + 1;

			// Ensure all internal nodes between root and leaf are created.
			for (int level = 0; level < numLevels; ++level) {
				var inode = selection.Internals [level];
				var index = ComputeIndexOfBlockAtLevel (blockId, level);
				if ((inode.Children [index] & FullNodeBlockIdBit) != 0) {
					// Already marked as used!
					throw new InvalidOperationException ("The specified block is already marked as used.");
				} else if (inode.Children[index] != EmptyNodeBlockId) {
					selection.Select (level, index);
				} else {
					// Need to create a child block.
					selection.Select (level, index);

					using (var handle = db.BufferPool.CreateHandle ()) {
						var buffer = handle.Buffer;
						if (level == numLevels - 1) {
							// Create leaf node.
							var bmp = selection.Leaf.Bitmap;
							++numCreatedNodes; // leaf node
							if (numCreatedNodes + 1 > numBlocksInLeafNode) {
								throw new InvalidOperationException ();
							}
							storage.NumBlocks = Math.Max (storage.NumBlocks, nextNodeBlockId + 1);

							bmp.FillOne ();
							bmp.SetRanged (false, 0, numCreatedNodes + 2);

							Buffer.BlockCopy (bmp.GetBuffer (), 0, buffer, 0, buffer.Length);
							selection.Leaf.BlockId = nextNodeBlockId;
							selection.Leaf.Dirty = false;
							storage.WriteBlock (nextNodeBlockId, buffer, 0);

							// Link the new leaf node.
							inode.Children [index] = nextNodeBlockId;
							inode.Dirty = true;

							++db.Superblock.NumAllocatedBlocks;
							db.Superblock.Write ();
							return;
						} else {
							// Create internal node.
							var ninode = selection.Internals [level + 1];
							if (numCreatedNodes + 2 > numBlocksInLeafNode) {
								throw new InvalidOperationException ();
							}
							storage.NumBlocks = Math.Max (storage.NumBlocks, nextNodeBlockId + 1);
							ninode.InitializeEmpty (nextNodeBlockId);

							// Link the new node.
							inode.Children [index] = nextNodeBlockId;
							inode.Dirty = true;

							++nextNodeBlockId;
							++numCreatedNodes;
						}
					}
				}
			}

			if (numCreatedNodes > 0) {
				throw new InvalidOperationException ();
			}

			// There already was a leaf node.
			var leaf = selection.Leaf;
			var leafIndex = ComputeIndexOfBlockAtLevel (blockId, numLevels);
			if (!leaf.Bitmap[leafIndex]) {
				throw new InvalidOperationException ("The specified block is already marked as used.");
			}
			leaf.Bitmap [leafIndex] = false;
			leaf.Dirty = true;

			// All blocks in the leaf were marked as used?
			if (leaf.Bitmap.NumOnes == 0) {
				for (int level = numLevels - 1; level >= 0; --level) {
					var inode = selection.Internals [level];
					inode.Children [inode.SelectedIndex] |= FullNodeBlockIdBit;
					inode.Dirty = true;

					if (!inode.IsFull()) {
						break;
					}
				}
			}

			++db.Superblock.NumAllocatedBlocks;
			db.Superblock.Write ();
		}

		void MarkBlockAsFree(long blockId)
		{
			if (blockId == 0) {
				throw new InvalidOperationException ("Superblock cannot be freed.");
			}

			for (int level = 0; level < numLevels; ++level) {
				var inode = selection.Internals [level];
				var index = ComputeIndexOfBlockAtLevel (blockId, level);
				if (inode.Children [index] == EmptyNodeBlockId) {
					// Already marked as used!
					throw new InvalidOperationException ("The specified block is already marked as free.");
				} else {
					selection.Select (level, index);
				}
			}

			// Update the leaf node.
			var leaf = selection.Leaf;
			var leafIndex = ComputeIndexOfBlockAtLevel (blockId, numLevels);
			if (leaf.Bitmap[leafIndex]) {
				throw new InvalidOperationException ("The specified block is already marked as used.");
			}
			leaf.Bitmap [leafIndex] = true;
			leaf.Dirty = true;

			if (leaf.Bitmap.NumOnes == 1) {
				// Leaf was full, but is not full now.
				for (int level = numLevels - 1; level >= 0; --level) {
					var inode = selection.Internals [level];
					if ((inode.Children [inode.SelectedIndex] & FullNodeBlockIdBit) == 0) {
						break;
					}
					inode.Children [inode.SelectedIndex] &= ~FullNodeBlockIdBit;
					inode.Dirty = true;
				}
			}

			--db.Superblock.NumAllocatedBlocks;
			db.Superblock.Write ();
		}

		// traverses tree to find a free block.
		long? TraverseAndFindFreeBlock(int level)
		{
			if (level == numLevels) {
				// leaf level.
				var leaf = selection.Leaf;
				var bmp = leaf.Bitmap;
				if (bmp.NumOnes == 0) {
					return null;
				} else {
					var r = bmp.FindFirstOne () + selection.GetLeafStartingBlockId();
					if (r >= Superblock.DatabaseSize) {
						return null;
					}
					return r;
				}
			} else {
				var inode = selection.Internals [level];
				int startIndex = inode.SelectedIndex;
				if (startIndex == -1) {
					startIndex = 0;
				}
				int numChildren = numChildrenInNode;
				for (int i = 0; i < numChildren; ++i) {
					int index = (i + startIndex) & (numChildren - 1);
					long bId = inode.Children [index];
					if (bId == EmptyNodeBlockId) {
						// this region is completely empty
						selection.Select (level, index);
						var r = selection.GetInternalNodeStartingBlockId (level + 1);
						if (r >= Superblock.DatabaseSize) {
							continue;
						}
						return r;
					} else if ((bId & FullNodeBlockIdBit) == 0) {
						// partially empty
						selection.Select (level, index);
						var ret = TraverseAndFindFreeBlock (level + 1);
						if (ret != null) {
							return ret;
						}
					}
				}
				return null;
			}
		}

		long? TryAllocateBlock() 
		{
			var ret = TraverseAndFindFreeBlock (0);
			if (ret == null) {
				return null;
			}
			MarkBlockAsUsed ((long)ret);

			return ret;
		}

		long AllocateBlockImpl() 
		{
			var ret = TryAllocateBlock ();
			if (ret != null) {
				return (long)ret;
			}

			// No free space. Expand the database and retry.
			ResizeDatabase (Superblock.DatabaseSize + 16);
			ret = TryAllocateBlock ();
			if (ret != null) {
				return (long)ret;
			}

			throw new InvalidOperationException ("Expanding database did not make a room for a new block.");
		}

		public long AllocateBlock() 
		{
			var ret = AllocateBlockImpl ();
			#if Validate
			if (onmemory.Contains(ret)) {
				throw new InvalidOperationException();
			}
			onmemory.Add(ret);
			#endif
			return ret;
		}


		public void DeallocateBlock(long blockId) 
		{
			#if Validate
			if(!onmemory.Remove(blockId)) {
				throw new InvalidOperationException();
			}
			#endif
			MarkBlockAsFree (blockId);
		}

		public void Flush()
		{
			selection.Flush ();
		}

		/// <summary>
		/// Resizes the database.
		/// If any errors should occur during the resize process, 
		/// database might be left in an inconsistent state.
		/// This is not serious as this usually results in wasted spaces, but
		/// to avoid this, transactional storage (e.g. WalBlockFile) is
		/// recommended.
		/// </summary>
		/// <param name="newNumBlocks">New database size in number of blocks.</param>
		void ResizeDatabase(long newNumBlocks) 
		{
			if (newNumBlocks < Superblock.DatabaseSize) {
				return;
			}

			int newNumLevels = ComputeNumLevelsForDatabaseSize (newNumBlocks);
			if (newNumLevels == numLevels) {
				// The number of levels didn't change.
				// Ne need to update freemap.
				storage.NumBlocks = newNumBlocks;
				Superblock.DatabaseSize = newNumBlocks;
				Superblock.Write ();
				return;
			}

			selection.Flush ();

			// The number of levels changed.
			int numAddedLevels = newNumLevels - numLevels;
			if (numAddedLevels < 0) {
				throw new InvalidOperationException ("Number of freemap levels decreased.");
			}

			// block ids for new nodes
			long[] addedBlockIds = new long[numAddedLevels];
			{
				int i = 0;
				for (; i < numAddedLevels; ++i) {
					var bId = TryAllocateBlock ();
					if (bId != null) {
						addedBlockIds [i] = (long)bId;
					} else {
						break;
					}
				}
				selection.Flush ();

				if (i < numAddedLevels) {
					// No space left for new nodes.
					// Since we are expanding the database, we may be able to allocate blocks from
					// the expanded area...
					int shortage = i - numAddedLevels;
					long requiredDatabaseSize = checked(Superblock.DatabaseSize + shortage);
					if (ComputeNumLevelsForDatabaseSize(requiredDatabaseSize) > newNumLevels) {
						// even more levels will be required.
						// retry this process all over again.
						for (--i; i >= 0; --i) {
							DeallocateBlock (addedBlockIds [i]);
						}
						ResizeDatabase (requiredDatabaseSize);
						return;
					}

					// We can allocate blocks from the expanded area.
					newNumBlocks = Math.Max (newNumBlocks, requiredDatabaseSize);
					long bId = Superblock.DatabaseSize;
					for (; i < numAddedLevels; ++i) {
						addedBlockIds [i] = bId++;
					}
				}

			}

			// resize storage.
			storage.NumBlocks = Math.Max (storage.NumBlocks, newNumBlocks);

			// make new internal nodes.
			long oldDbSize = Superblock.DatabaseSize;
			using (var handle = db.BufferPool.CreateHandle ()) {
				var buf = handle.Buffer;
				long[] buf2 = new long[storage.BlockSize / 8];

				for (int i = 0; i < buf2.Length; ++i) {
					buf2 [i] = EmptyNodeBlockId;
				}
				Buffer.BlockCopy (buf2, 0, buf, 0, buf.Length);

				long currentRootNode = Superblock.RootFreemapBlock;
				for (int i = 0; i < numAddedLevels; ++i) {
					buf2 [0] = currentRootNode;
					Buffer.BlockCopy (buf2, 0, buf, 0, 8);
					storage.WriteBlock (addedBlockIds [i], buf, 0);
					currentRootNode = addedBlockIds [i];
				}

				numLevels = newNumLevels;
				Superblock.DatabaseSize = newNumBlocks;
				Superblock.RootFreemapBlock = currentRootNode;

				Superblock.Write ();
			}

			// recreate selection.
			selection = new Selection (this);

			// allocate blocks for freemap which are not allocated yet.
			foreach (long blockId in addedBlockIds) {
				if (blockId >= oldDbSize) {
					MarkBlockAsUsed (blockId);
				}
			}

		}
	}
}


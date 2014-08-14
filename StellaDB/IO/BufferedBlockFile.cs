using System;
using System.Collections.Generic;

namespace Yavit.StellaDB.IO
{
	public class BufferedBlockFile: IBlockStorage
	{
		private readonly IBlockStorage baseBlockFile;

		private sealed class Block
		{
			public long blockId;
			public byte[] bytes;
			public bool dirty = false;
		}

		private LinkedList<Block> blocks = new LinkedList<Block> ();
		private Dictionary<long, LinkedListNode<Block>> blockMap = 
			new Dictionary<long, LinkedListNode<Block>>();
		private List<LinkedListNode<Block>> bufferPool = 
			new List<LinkedListNode<Block>>();

		private int capacity;

		public BufferedBlockFile (IBlockStorage baseBlockFile, int capacity = 64)
		{
			this.baseBlockFile = baseBlockFile;
			this.capacity = capacity;

			if (baseBlockFile == null) {
				throw new ArgumentNullException ("baseBlockFile");
			}
		}

		public int Capacity 
		{
			get {
				return capacity;
			}
			set {
				if (value < 0) {
					throw new ArgumentOutOfRangeException ("value");
				}
				capacity = value;
				CheckCapacity ();
				bufferPool.Clear ();
			}
		}

		void Writeback(Block block)
		{
			if (!block.dirty) {
				return;
			}
			// write-through
			baseBlockFile.WriteBlock (block.blockId, block.bytes, 0);
			block.dirty = false;
		}

		private void CheckCapacity() {
			while (blocks.Count > capacity) {
				var last = blocks.Last;
				blockMap.Remove (last.Value.blockId);
				Writeback (last.Value);
				bufferPool.Add (last);
				blocks.RemoveLast ();
			}
		}

		private Block LoadBlock(long blockId, bool noRead)
		{
			LinkedListNode<Block> node;

			if (!blockMap.TryGetValue(blockId, out node)) {
				// not found

				if (bufferPool.Count == 0) {
					node = new LinkedListNode<Block> (new Block());
					node.Value.bytes = new byte[baseBlockFile.BlockSize];
				} else {
					node = bufferPool [bufferPool.Count - 1];
					bufferPool.RemoveAt (bufferPool.Count - 1);
				}
				node.Value.blockId = blockId;
				node.Value.dirty = false;
				blocks.AddFirst (node);
				if (!noRead) {
					baseBlockFile.ReadBlock (blockId, node.Value.bytes, 0);
				}

				blockMap.Add (blockId, node);
				CheckCapacity ();
			} else {
				// found
				if (node != blocks.First) {
					// move to front
					blocks.Remove (node);
					blocks.AddFirst (node);
				}
			}

			return node.Value;
		}

		public void ReadBlock (long blockId, byte[] buffer, int offset)
		{
			if (capacity == 0) {
				// not cached
				baseBlockFile.ReadBlock (blockId, buffer, offset);
				return;
			}

			var block = LoadBlock (blockId, false);

			Buffer.BlockCopy (block.bytes, 0, buffer, offset, block.bytes.Length);
		}

		public void WriteBlock (long blockId, byte[] buffer, int offset)
		{
			baseBlockFile.NumBlocks = Math.Max (baseBlockFile.NumBlocks, blockId + 1);

			if (capacity == 0) {
				// not cached
				// write-through
				baseBlockFile.WriteBlock (blockId, buffer, offset);
				return;
			}

			var block = LoadBlock (blockId, true);
			block.dirty = true;
			Buffer.BlockCopy (buffer, offset, block.bytes, 0, block.bytes.Length);
		}

		public void Flush ()
		{
			foreach (var block in blocks) {
				Writeback (block);
			}
			baseBlockFile.Flush ();
		}

		public int BlockSize {
			get {
				return baseBlockFile.BlockSize;
			}
		}

		public long NumBlocks {
			get {
				return baseBlockFile.NumBlocks;
			}
			set {
				long curNumBlocks = baseBlockFile.NumBlocks;
				if (value < curNumBlocks) {
					// truncated.
					for (var it = blocks.First; it != null;) {
						var cur = it; it = it.Next;
						if (cur.Value.blockId >= value) {
							blockMap.Remove (cur.Value.blockId);
							blocks.Remove (cur);
						}
					}
				}
				baseBlockFile.NumBlocks = value;
			}
		}


	}
}


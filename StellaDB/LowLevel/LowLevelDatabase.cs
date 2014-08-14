using System;

namespace Yavit.StellaDB.LowLevel
{
	public class LowLevelDatabase
	{
		public readonly StellaDB.IO.IBlockStorage Storage;

		internal readonly Superblock Superblock;
		internal readonly Freemap Freemap;
		internal readonly LinkedListBlob.HeaderManager LinkedListBlobHeaderManager;
		internal readonly LinkedListBlob.BlockManager LinkedListBlobManager;

		internal readonly BufferPool BufferPool;

		// Protect LowLevelDatabase from concurrent access
		// TODO: use this (sync) everywhere
		internal readonly object sync = new object();

		public LowLevelDatabase (StellaDB.IO.IBlockStorage storage)
		{
			Storage = storage;
			if (storage == null) {
				throw new ArgumentNullException ("storage");
			}

			if (!InternalUtils.IsPowerOfTwo(storage.BlockSize)) {
				throw new InvalidOperationException ("Block size must be a power of two.");
			}

			if (storage.BlockSize < 64) {
				throw new InvalidOperationException ("Block size must be at least 64 bytes.");
			}

			if (storage.BlockSize > 65536) {
				throw new InvalidOperationException ("Block size must be at most 65536 bytes.");
			}

			if (!BitConverter.IsLittleEndian) {
				throw new PlatformNotSupportedException ("Big-endian system is not supported (yet).");
			}

			BufferPool = new BufferPool (storage.BlockSize);

			Superblock = new Superblock (Storage);
			Freemap = new Freemap (this);
			LinkedListBlobHeaderManager = new LinkedListBlob.HeaderManager (this);
			LinkedListBlobManager = new LinkedListBlob.BlockManager (this);

			Flush ();
		}

		public long NumAllocatedBlocks
		{
			get {
				return Superblock.NumAllocatedBlocks;
			}
		}

		Utils.WeakValueDictionary<long, BTree> btrees = 
			new Utils.WeakValueDictionary<long, BTree>();
		public BTree OpenBTree(long blockId)
		{
			if (blockId <= 0) {
				throw new ArgumentOutOfRangeException ("blockId", "Block ID must be positive.");
			}

			BTree tree;
			if (btrees.TryGetValue(blockId, out tree)) {
				return tree;
			}

			tree = new BTree (this, blockId);
			btrees.Add (blockId, tree);
			return tree;
		}

		public BTree CreateBTree(BTreeParameters param)
		{
			var tree = new BTree (this, -1, param);
			btrees.Add (tree.BlockId, tree);
			return tree;
		}

		public BTree CreateBTree()
		{
			return CreateBTree (new BTreeParameters ());
		}

		Utils.WeakValueDictionary<long, Blob> blobs = 
			new Utils.WeakValueDictionary<long, Blob>();
		public Blob OpenBlob(long blockId)
		{
			if (blockId <= 0) {
				throw new ArgumentOutOfRangeException ("blockId", "Block ID must be positive.");
			}

			Blob blob;
			if (blobs.TryGetValue(blockId, out blob)) {
				return blob;
			}

			blob = new Blob (this, blockId);
			blobs.Add (blockId, blob);
			return blob;
		}
		public Blob CreateBlob()
		{
			var blob = new Blob (this, -1);
			blobs.Add (blob.BlockId, blob);
			return blob;
		}

		public void Flush()
		{
			Superblock.Write ();
			Freemap.Flush ();
			Storage.Flush ();
		}
	}
}


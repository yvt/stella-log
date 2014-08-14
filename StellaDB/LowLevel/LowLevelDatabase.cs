using System;

namespace Yavit.StellaDB.LowLevel
{
	public class LowLevelDatabaseParameters
	{
		int numCachedBlocks = 16;
		public int NumCachedBlocks
		{
			get { return numCachedBlocks; }
			set {
				if (value < 0) {
					throw new InvalidOperationException ();
				}
				numCachedBlocks = value;
			}
		}
	}

	public class LowLevelDatabase
	{
		readonly StellaDB.IO.IBlockStorage Storage;
		internal readonly StellaDB.IO.Pager Pager;

		internal readonly Superblock Superblock;
		internal readonly Freemap Freemap;
		internal readonly LinkedListBlob.HeaderManager LinkedListBlobHeaderManager;
		internal readonly LinkedListBlob.BlockManager LinkedListBlobManager;

		internal readonly BufferPool BufferPool;

		// Protect LowLevelDatabase from concurrent access
		// TODO: use this (sync) everywhere
		internal readonly object sync = new object();

		public LowLevelDatabase (StellaDB.IO.IBlockStorage storage,
			LowLevelDatabaseParameters param)
		{
			Storage = storage;
			if (storage == null) {
				throw new ArgumentNullException ("storage");
			}

			Pager = new StellaDB.IO.Pager (storage, param.NumCachedBlocks + 1);

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

			Superblock = new Superblock (Pager);
			Freemap = new Freemap (this);
			LinkedListBlobHeaderManager = new LinkedListBlob.HeaderManager (this);
			LinkedListBlobManager = new LinkedListBlob.BlockManager (this);

			Flush ();
		}

		public LowLevelDatabase (StellaDB.IO.IBlockStorage storage):
		this(storage, new LowLevelDatabaseParameters())
		{ }

		public long NumAllocatedBlocks
		{
			get {
				return Superblock.NumAllocatedBlocks;
			}
		}

		public long UserBlockId1
		{
			get {
				return Superblock.UserBlockId1;
			}
			set {
				Superblock.UserBlockId1 = value;
			}
		}
		public long UserBlockId2
		{
			get {
				return Superblock.UserBlockId2;
			}
			set {
				Superblock.UserBlockId2 = value;
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
			Pager.Flush ();
			Storage.Flush ();
		}
	}
}


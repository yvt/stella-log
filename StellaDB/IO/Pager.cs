using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.IO
{
	sealed class Pager
	{

		// Every page is in one of these states:
		//                                                    
		// Unloaded: page is not associated to any block Id.
		// Loaded: page is not loaded, but not pinned.
		// Pinned: page is loaded and pinned.
		// 
		//            freePagePool   unpinnedPages
		// Unloaded        X
		// Loaded                          X
		// Pinned
		//
		// Loaded/Pinned nodes can be dirty node and such nodes are added to
		// dirtyList.
		internal sealed class Page: IDisposable
		{
			readonly public Pager Pager;
			public long? BlockId { get; private set; }
			public long PinCount { get; private set; }
			public LinkedListNode<Page> Node;
			public byte[] Bytes { get; private set; }
			public bool Dirty { get; private set; }
			public LinkedListNode<Page> DirtyListNode;

			public Page(Pager pager)
			{
				Pager = pager;
				Bytes = new byte[Pager.BlockSize];
			}

			public void Dispose ()
			{
				Bytes = null;
				BlockId = null;
			}

			public void Load(long blockId, bool erase)
			{
				Unload ();

				if (!erase) {
					Pager.Storage.ReadBlock (blockId, Bytes, 0);
				}
				BlockId = blockId;
				Dirty = false;
				if (Pager.pageTable.ContainsKey(blockId)) {
					throw new InvalidOperationException ("Page is already added to pageTable.");
				}
				Pager.pageTable.Add (blockId, Node);
				Pager.freePagePool.Remove (Node);
				Pager.unpinnedPages.AddFirst (Node);
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void Pin()
			{
				if (BlockId == null) {
					throw new InvalidOperationException ("Page is not loaded.");
				}
				++PinCount;
				if (PinCount == 1) {
					Pager.unpinnedPages.Remove (Node);
				}
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void Unpin()
			{
				if (BlockId == null) {
					throw new InvalidOperationException ("Page is not loaded.");
				}
				--PinCount;
				if (PinCount == 0) {
					Pager.unpinnedPages.AddFirst (Node);
					Pager.CheckCapacity ();
				}
				if (PinCount < 0) {
					throw new InvalidOperationException ("Double unpinning.");
				}
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void Flush()
			{
				if (!Dirty) {
					return;
				}
				Pager.Storage.WriteBlock ((long)BlockId, Bytes, 0);
				Dirty = false;
				Pager.dirtyPages.Remove (DirtyListNode);
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void MarkAsDirty()
			{
				if (Dirty) {
					return;
				}
				Dirty = true;
				Pager.dirtyPages.AddLast (DirtyListNode);
			}

			public void Unload()
			{
				if (PinCount > 0) {
					throw new InvalidOperationException ("Cannot unload a pinned page.");
				}
				if (BlockId != null) {
					Flush ();
					Pager.pageTable.Remove ((long)BlockId);
					Pager.unpinnedPages.Remove (Node);
					Pager.freePagePool.AddFirst (Node);
					Pager.CheckCapacity ();
				}
				BlockId = null;
			}

		}

		readonly LinkedList<Page> unpinnedPages = new LinkedList<Page> ();
		readonly Dictionary<long, LinkedListNode<Page>> pageTable = 
			new Dictionary<long, LinkedListNode<Page>>();

		readonly LinkedList<Page> freePagePool = new LinkedList<Page> ();

		readonly LinkedList<Page> dirtyPages = new LinkedList<Page> ();

		readonly public IBlockStorage Storage;

		// Total number of unpinnedPages and freePagePool allowed. Should be at least 1.
		readonly int maxCacheBlocks;

		readonly public int BlockSize;

		public Pager (IBlockStorage storage, int maxCacheBlocks)
		{
			if (storage == null)
				throw new ArgumentNullException ("storage");

			if (maxCacheBlocks < 1)
				throw new ArgumentOutOfRangeException ("maxCacheBlocks");

			Storage = storage;
			BlockSize = Storage.BlockSize;
			this.maxCacheBlocks = maxCacheBlocks;
		}

		public long NumBlocks
		{
			get { return Storage.NumBlocks; }
			set { Storage.NumBlocks = value; }
		}

		Page EnsureFreePage()
		{
			if (freePagePool.Count == 0) {
				if (unpinnedPages.Count >= maxCacheBlocks) {
					unpinnedPages.Last.Value.Unload ();
					return unpinnedPages.Last.Value;
				} else {
					// Create page
					var page = new Page (this);
					page.Node = new LinkedListNode<Page> (page);
					page.DirtyListNode = new LinkedListNode<Page> (page);
					freePagePool.AddLast (page.Node);
					return page;
				}
			} else {
				return freePagePool.First.Value;
			}
		}

		public void Flush()
		{
			while (dirtyPages.Count > 0) {
				dirtyPages.First.Value.Flush ();
			}
		}

		void CheckCapacity()
		{
			int maxFreePagePoolCunt = Math.Max(maxCacheBlocks - unpinnedPages.Count, 0);
			while (freePagePool.Count > maxFreePagePoolCunt) {
				freePagePool.Last.Value.Dispose ();
				freePagePool.RemoveLast ();
			}
			while (unpinnedPages.Count > maxCacheBlocks) {
				var vl = unpinnedPages.Last.Value;
				vl.Flush ();
				pageTable.Remove ((long)vl.BlockId);
				vl.Dispose ();
				unpinnedPages.RemoveLast ();
			}
		}

		public PageHandle this[long blockId]
		{
			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			get {
				return GetPageHandle (blockId);
			}
		}

		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public PageHandle GetPageHandle(long blockId, bool erased = false)
		{
			LinkedListNode<Page> node;
			if (pageTable.TryGetValue(blockId, out node)) {
				return new PageHandle (node.Value);
			} else {
				var page = EnsureFreePage ();
				page.Load(blockId, erased);
				return new PageHandle (page);
			}
		}


		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public PinnedPage Pin(long blockId)
		{
			return this [blockId].Pin ();
		}


		[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
		public PinnedPage EraseAndPin(long blockId)
		{
			// TODO: optimize EraseAndPin
			return Pin (blockId);
		}



		public struct PinnedPage: IDisposable
		{
			Page page;

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			internal PinnedPage(Page page)
			{
				this.page = page;
				page.Pin();
			}

			public byte[] Bytes
			{
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					return page.Bytes;
				}
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void MarkAsDirty()
			{
				page.MarkAsDirty ();
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void Dispose ()
			{
				if (page == null) {
					return;
				}
				page.Unpin ();
				page = null;
			}

			public bool IsValid
			{
				get { return page != null; }
			}
		}

		public struct PageHandle
		{
			readonly Page page;
			readonly long blockId;

			internal PageHandle(Page page)
			{
				this.page = page;
				blockId = (long) page.BlockId;
			}

			public PinnedPage Pin()
			{
				CheckValid ();
				return new PinnedPage (page);
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			private void CheckValid()
			{
				if (page.BlockId != blockId) {
					throw new ObjectDisposedException ("PageHandle");
				}
			}

			public byte[] Bytes
			{
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					CheckValid ();
					return page.Bytes;
				}
			}

			[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
			public void MarkAsDirty()
			{
				page.MarkAsDirty ();
			}

		}
	}
}


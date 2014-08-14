using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.LowLevel
{

	sealed class LinkedListBlob
	{
		/*
		 *   Block Design
		 *  B = Block Size
		 *  
		 *  0  ~ 3      Header Magic
		 *  4  - 7      Length of Data in This Block
		 *  8  - 15     Previous Block Id
		 *  16 - 23     Next Block Id
		 *  24 - B - 1  Data (At most B - 24 bytes)
		 *  
		 */
		public readonly LowLevelDatabase Database;


		internal sealed class HeaderManager: Utils.SharedResourceManager<Header, long, long>
		{
			public readonly LowLevelDatabase Database;

			public HeaderManager(LowLevelDatabase db)
			{
				Database = db;
			}

			protected override Header CreateResource ()
			{
				return new Header (Database);
			}
		}

		internal sealed class Header: Utils.SharedResource<long, long>
		{
			public readonly LowLevelDatabase Database;
			public long? BlockId { get; private set; }

			public long Length {
				get {
					ComputeTailRef ();
					return TailRef.Resource.Position + TailRef.Resource.DataLength;
				}
			}
			public BlockRef TailRef;

			public Header(LowLevelDatabase db)
			{
				Database = db;
				TailRef = new BlockRef(db.LinkedListBlobManager);
			}

			public override long? ResourceId {
				get {
					return BlockId;
				}
			}

			public override void Load (long id, long category)
			{
				Unload ();
				BlockId = id;
			}
			public override void Initialize (long id, long category)
			{
				Unload ();
				BlockId = id;
			}

			public void ComputeTailRef()
			{
				TailRef.SeekToEnd ((long)BlockId);
			}

			public int MaxDataLength {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					return Database.LinkedListBlobManager.MaxDataLength;
				}
			}

			public void AppendBlock(BlockRef tmp)
			{
				ComputeTailRef ();

				long newBlockId = Database.Freemap.AllocateBlock ();
				tmp.InitializeAndLoad (newBlockId, (long)BlockId);
				tmp.Resource.PreviousBlockId = (long)TailRef.Resource.BlockId;
				tmp.Resource.Position = TailRef.Resource.Position + MaxDataLength;
				TailRef.Resource.NextBlockId = newBlockId;
				TailRef.Load (newBlockId, (long)BlockId);
				if (TailRef.Resource.Position == 0) {
					throw new InvalidOperationException ();
				}
			}

			public void AppendBlock()
			{
				using (var tmp = new BlockRef (Database.LinkedListBlobManager)) {
					AppendBlock (tmp);
				}
			}

			public void SetLength(long len)
			{
				if (len < 0) {
					throw new ArgumentOutOfRangeException ("len");
				}

				ComputeTailRef ();

				using (var tmp = new BlockRef(Database.LinkedListBlobManager)) {
					// truncate
					while (len <= TailRef.Resource.Position &&
						TailRef.Resource.Position > 0) {
						long p = TailRef.Resource.Position;
						tmp.Load (TailRef.Resource.PreviousBlockId, (long)BlockId);
						tmp.Resource.NextBlockId = 0;
						TailRef.Resource.Drop (true);
						TailRef.Load ((long)tmp.Resource.BlockId, (long)BlockId);
						TailRef.Resource.Position = p - MaxDataLength;
					}

					// expand
					while (len > checked(TailRef.Resource.Position + MaxDataLength)) {
						AppendBlock (tmp);
					}

					// adjust last block
					var tail = TailRef.Resource;
					int prefLen = checked((int)(len - tail.Position));
					tail.Resize (prefLen);
				}
			}

			public override void Unload ()
			{
				BlockId = null;
				TailRef.Unload ();
			}
		}

		internal sealed class HeaderRef: Utils.SharedResourceRef<Header, long, long>
		{
			public HeaderRef(HeaderManager manager): base(manager) { }
		}

		internal sealed class BlockManager: Utils.SharedResourceManager<Block, long, long>
		{
			public readonly LowLevelDatabase Database;

			public BlockManager(LowLevelDatabase db)
			{
				Database = db;
			}

			public int MaxDataLength {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					return Database.Storage.BlockSize - Block.BlockDataOffset;
				}
			}

			protected override Block CreateResource ()
			{
				return new Block (Database);
			}

			public void Flush()
			{
				foreach (var r in Resources) {
					r.Write ();
				}
			}
		}

		internal sealed class Block: Utils.SharedResource<long, long>
		{
			const uint HeaderMagic = 0x18018019;
			const int BlockDataLengthOffset = 4;
			const int BlockPreviousBlockIdOffset = 8;
			const int BlockNextBlockIdOffset = 16;
			public const int BlockDataOffset = 24;

			public readonly LowLevelDatabase Database;
			public long? BlockId { get; private set; }
			byte[] buffer;
			public long Position;
			InternalUtils.BitConverter BitCvt;
			bool Dirty = false;
			long blobId;

			public Block(LowLevelDatabase db)
			{
				Database = db;
				buffer = new byte[db.Storage.BlockSize];
				BitCvt = new InternalUtils.BitConverter(buffer);
			}

			public override long? ResourceId {
				get {
					return BlockId;
				}
			}

			public long BlobId {
				get {
					return blobId;
				}
			}

			public int DataLength {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					return BitCvt.GetInt32 (BlockDataLengthOffset);
				}
				set {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					BitCvt.Set (BlockDataLengthOffset, value); Dirty = true;
				}
			}
			public void Resize(int size)
			{
				if (size < 0 || size > MaxDataLength) {
					throw new ArgumentOutOfRangeException ("size");
				}
				int oldSize = DataLength;
				if (oldSize == size) {
					return;
				}

				// If we are expanding, zero-fill
				for (int i = oldSize + DataOffset, end = size + DataOffset; i < end; ++i) {
					buffer [i] = 0;
				}

				DataLength = size;
			}
			public long PreviousBlockId {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					return BitCvt.GetInt64 (BlockPreviousBlockIdOffset);
				}
				set {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					BitCvt.Set (BlockPreviousBlockIdOffset, value); Dirty = true;
				}
			}
			public long NextBlockId {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					return BitCvt.GetInt64 (BlockNextBlockIdOffset);
				}
				set {
					if (BlockId == null) 
						throw new InvalidOperationException ("Block is not loaded.");
					BitCvt.Set (BlockNextBlockIdOffset, value); Dirty = true;
				}
			}
			public int MaxDataLength {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					return Database.LinkedListBlobManager.MaxDataLength;
				}
			}
			public int DataOffset {
				[MethodImpl(InternalUtils.MethodImplAggresiveInlining)]
				get {
					return BlockDataOffset;
				}
			}
			public int Read(int offset, byte[] buf, int start, int length)
			{
				if (offset < 0) {
					throw new ArgumentOutOfRangeException ("offset");
				}
				if (start < 0) {
					throw new ArgumentOutOfRangeException ("start");
				}
				if (length < 0 || length + start > buf.Length) {
					throw new ArgumentOutOfRangeException ("length");
				}
				if (buf == null) {
					throw new ArgumentNullException ("buf");
				}
				int endOffset = checked(offset + length);
				length = Math.Min (endOffset, DataLength) - offset;
				Buffer.BlockCopy (buffer, offset + DataOffset, buf, start, length);
				return length;
			}
			public int Write(int offset, byte[] buf, int start, int length)
			{
				if (offset < 0) {
					throw new ArgumentOutOfRangeException ("offset");
				}
				if (start < 0) {
					throw new ArgumentOutOfRangeException ("start");
				}
				if (length < 0 || length + start > buf.Length) {
					throw new ArgumentOutOfRangeException ("length");
				}
				if (buf == null) {
					throw new ArgumentNullException ("buf");
				}
				int endOffset = checked(offset + length);
				length = Math.Min (endOffset, MaxDataLength) - offset;
				Buffer.BlockCopy (buf, start, buffer, offset + DataOffset, length);
				DataLength = Math.Max (offset + length, DataLength);
				Dirty = true;
				return length;
			}

			public override void Load(long id, long category)
			{
				Unload ();

				this.blobId = category;
				BlockId = id;

				try {

					Database.Storage.ReadBlock (id, buffer, 0);

					if (BitCvt.GetUInt32(0) != HeaderMagic) {
						throw new InvalidMagicNumberException ();
					}
					if (DataLength < 0 || DataLength > MaxDataLength) {
						throw new DataInconsistencyException ("Data length is invalid.");
					}

				} catch {
					BlockId = null;
					throw;
				}

				Dirty = false;
			}

			public override void Initialize (long id, long category)
			{
				Unload ();

				this.blobId = category;
				BlockId = id;

				BitCvt.Set (0, HeaderMagic);
				DataLength = 0;
				PreviousBlockId = 0;
				NextBlockId = 0;

				Dirty = true;
			}

			public override void Unload()
			{
				Write ();
				BlockId = null;
			}

			public void Write()
			{
				if (!Dirty || BlockId == null) {
					return;
				}
				Database.Storage.WriteBlock ((long)BlockId, buffer, 0);
				Dirty = false;
			}

			public void Drop(bool truncating)
			{
				long blockId = (long)BlockId;
				long prev = PreviousBlockId;
				Database.Freemap.DeallocateBlock (blockId);
				Dirty = false;
				if (truncating && prev != 0) {
					// move the existing reference to the previous block
					foreach (var r in Database.LinkedListBlobManager.GetReferencesOfResource(blockId)) {
						((BlockRef)r).MoveTo (prev, blobId, Position - MaxDataLength);
					}
				}
				Database.LinkedListBlobManager.InvalidateAllReferences (blockId);
				BlockId = null;
			}
		}

		internal sealed class BlockRef: Utils.SharedResourceRef<Block, long, long>
		{
			readonly BlockManager manager;
			public BlockRef(BlockManager manager): base(manager) 
			{
				this.manager = manager;
			}
			public void GoPrevious()
			{
				if (Resource.PreviousBlockId == 0) {
					throw new InvalidOperationException ("Cannot go previous");
				}
				MoveTo (Resource.PreviousBlockId, Resource.BlobId,
					Resource.Position - Resource.MaxDataLength);
			}
			public void GoNext()
			{
				if (Resource.NextBlockId == 0) {
					throw new InvalidOperationException ("Cannot go forward");
				}
				MoveTo (Resource.NextBlockId, Resource.BlobId,
					Resource.Position + Resource.MaxDataLength);
			}
			public void MoveTo(long blockId, long blobId, long position)
			{
				Load (blockId, blobId);
				Resource.Position = position;
			}
			public void SeekTo(long blobId, long position, bool writing)
			{
				// Round off
				int blockSize = manager.MaxDataLength;
				position = (position / blockSize) * blockSize;

				// Find nearest cursor to the target offset
				long bestBlockId = blobId;
				long bestDistance = position;
				if (Resource != null && Resource.BlobId == blobId &&
					Math.Abs(checked(position - Resource.Position)) < bestDistance) {
					bestBlockId = (long)Resource.BlockId;
					bestDistance = Math.Abs (position - Resource.Position);
				}
				foreach (var r in manager.GetResourcesOfCategory(blobId)) {
					if (bestDistance == 0) {
						break;
					}
					if (Math.Abs(checked(position - r.Position)) < bestDistance) {
						bestBlockId = (long)r.BlockId;
						bestDistance = Math.Abs (position - r.Position);
					}
				}

				Load (bestBlockId, blobId);
				if (bestBlockId == blobId) {
					Resource.Position = 0;
				} // for other cases, Resource.Position is already set

				// Go to the target offset
				while (position < Resource.Position) {
					GoPrevious ();
				}
				while (position > Resource.Position &&
					Resource.NextBlockId != 0) {
					GoNext ();
				}

				// Need to expand blob
				if (writing) {
					using (var tmp = new HeaderRef(Resource.Database.LinkedListBlobHeaderManager)) {
						tmp.Load (blobId, 0);
						while (position > Resource.Position) {
							Resource.Resize (Resource.DataLength);
							tmp.Resource.AppendBlock ();
							GoNext ();
						}
					}
				}
			}
			public void SeekToEnd(long blobId)
			{
				// Find nearest cursor to the target offset
				long bestBlockId = blobId;
				long bestPos = 0;
				if (Resource != null && Resource.BlobId == blobId &&
					Resource.Position > bestPos) {
					bestBlockId = (long)Resource.BlockId;
					bestPos = Resource.Position;
				}
				foreach (var r in manager.GetResourcesOfCategory(blobId)) {
					if (r.Position > bestPos) {
						bestBlockId = (long)r.BlockId;
						bestPos = r.Position;
					}
				}

				Load (bestBlockId, blobId);

				// Go to the target offset
				while (Resource.NextBlockId != 0) {
					GoNext ();
				}
			}
		}

		sealed class BlobStream: Stream
		{
			private long position = 0;
			private BlockRef block;
			private HeaderRef header;
			private LowLevelDatabase db;

			public BlobStream(LowLevelDatabase db, long blockId)
			{
				this.db = db;
				block = new BlockRef(db.LinkedListBlobManager);
				header = new HeaderRef(db.LinkedListBlobHeaderManager);

				header.Load(blockId, 0);
			}

			protected override void Dispose (bool disposing)
			{
				block.Dispose ();
				header.Dispose ();
				base.Dispose (disposing);
			}

			Header Header
			{
				get {
					var h = header.Resource;
					if (h == null) {
						throw new ObjectDisposedException ("Blob stream was invalidated or closed.");
					}
					return h;
				}
			}

			long BlobId
			{
				get {
					return (long)Header.BlockId;
				}
			}
			public override int Read (byte[] buffer, int offset, int count)
			{
				if (buffer == null) {
					throw new ArgumentNullException ("buffer");
				}
				if (offset < 0 || offset + count > buffer.Length) {
					throw new ArgumentOutOfRangeException ();
				}
				int readCount = 0;
				while (count > 0) {
					block.SeekTo (BlobId, position, false);

					if (position >= checked(block.Resource.Position + block.Resource.DataLength)) {
						break;
					}

					long blockPos = block.Resource.Position;
					int inBlockPos = checked((int)(position - blockPos));
					int inBlockMaxPos = block.Resource.MaxDataLength - inBlockPos;
					int rCount = block.Resource.Read(inBlockPos, buffer, offset, count);
					readCount += rCount;
					position += rCount;
					count -= rCount;
					offset += rCount;
					if (rCount < inBlockMaxPos) {
						// reached EOF
						break;
					}
				}
				return readCount;
			}
			public override void Write (byte[] buffer, int offset, int count)
			{
				if (buffer == null) {
					throw new ArgumentNullException ("buffer");
				}
				if (offset < 0 || offset + count > buffer.Length) {
					throw new ArgumentOutOfRangeException ();
				}

				while (count > 0) {
					block.SeekTo (BlobId, position, true);

					long blockPos = block.Resource.Position;
					int inBlockPos = checked((int)(position - blockPos));
					int inBlockMaxPos = block.Resource.MaxDataLength - inBlockPos;
					int inBlockWriteCount = Math.Min (count, inBlockMaxPos - inBlockPos);
					block.Resource.Write (inBlockPos, buffer, offset, inBlockWriteCount);
					position += inBlockWriteCount;
					count -= inBlockWriteCount;
					offset += inBlockWriteCount;
				}
			}
			public override bool CanRead {
				get { return true; }
			}
			public override bool CanTimeout {
				get { return false; }
			}
			public override bool CanWrite {
				get { return true; }
			}
			public override bool CanSeek {
				get { return true; }
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
			public override void Flush ()
			{ 
				foreach (var r in db.LinkedListBlobManager.GetResourcesOfCategory(BlobId)) {
					r.Write ();
				}
			}
			public override long Length {
				get {
					var header = Header;
					return header.Length;
				}
			}
			public override void SetLength (long value)
			{
				Header.SetLength (value);
			}
			public override long Position {
				get {
					return position;
				}
				set {
					if (value < 0) {
						throw new IOException ("Attempted to seek to the negative offset.");
					}
					position = value;
				}
			}
		}

		public long BlockId { get; private set; }

		internal LinkedListBlob (LowLevelDatabase db, long blockId = -1)
		{
			Database = db;
			if (db == null) {
				throw new ArgumentNullException ("db");
			}

			if (blockId == -1) {
				// create new empty BLOB
				BlockId = db.Freemap.AllocateBlock ();
				using (var r = new BlockRef(db.LinkedListBlobManager)) {
					r.InitializeAndLoad (BlockId, BlockId);
					r.Resource.Position = 0;
				}
			} else {
				// use existing BLOB
				BlockId = blockId;
			}
		}

		public Stream OpenStream()
		{
			if (BlockId == 0) {
				throw new ObjectDisposedException (GetType().Name);
			}
			return new BlobStream (Database, BlockId);
		}

		public void Drop()
		{
			if (BlockId == 0) {
				throw new ObjectDisposedException (GetType().Name);
			}

			// Trunctate BLOB to zero bytes. Only the header block will remain.
			using (var h = new HeaderRef(Database.LinkedListBlobHeaderManager)) {
				h.Load (BlockId, 0);
				h.Resource.SetLength (0);
			}
			Database.LinkedListBlobManager.InvalidateAllReferences (BlockId);
			foreach (var r in Database.LinkedListBlobHeaderManager.GetReferencesOfResource(BlockId)) {
				r.Unload ();
			}
			Database.Freemap.DeallocateBlock (BlockId);
			BlockId = 0;
		}
	}
}


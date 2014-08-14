using System;
using System.IO;

namespace Yavit.StellaDB.LowLevel
{
	class Superblock
	{
		const uint HeaderMagicSignature = 0x810893ff;
		const uint Version = 0x00000001;

		readonly IO.Pager.PinnedPage page;

		InternalUtils.BitConverter bc;

		public long RootFreemapBlock { 
			get { return bc.GetInt64 (12); }
			set { bc.Set (12, value); page.MarkAsDirty (); } 
		}

		/// <summary>
		/// Number of valid (allocated or not allocated) blocks in the database.
		/// Changing this value might require structure of freemap tree to be changed,
		/// so be careful.
		/// </summary>
		public long DatabaseSize { 
			get { return bc.GetInt64 (20); }
			set { bc.Set (20, value); page.MarkAsDirty (); } 
		}

		public long NumAllocatedBlocks { 
			get { return bc.GetInt64 (28); }
			set { bc.Set (28, value); page.MarkAsDirty (); } 
		}

		public long UserBlockId1 { 
			get { return bc.GetInt64 (36); }
			set { bc.Set (36, value); page.MarkAsDirty (); } 
		}
		public long UserBlockId2 { 
			get { return bc.GetInt64 (44); }
			set { bc.Set (44, value); page.MarkAsDirty (); } 
		}

		public Superblock (StellaDB.IO.Pager pager)
		{
			page = pager.Pin (0);

			bc = new InternalUtils.BitConverter (page.Bytes);

			uint magic = bc.GetUInt32 (0);
			if (magic == HeaderMagicSignature) {
				// header found.
				uint version = bc.GetUInt32 (4);
				if (version != Version) {
					throw new InvalidFormatVersionException ("Invalid database version number.");
				}

				int blockSize = bc.GetInt32 (8);
				if (blockSize != pager.BlockSize) {
					throw new DataInconsistencyException ("Block size mismatch.");
				}

				if (RootFreemapBlock >= pager.NumBlocks ||
					RootFreemapBlock <= 0) {
					throw new DataInconsistencyException ("Invalid root freemap block index.");
				}

				if (DatabaseSize < 0 ||
					DatabaseSize > pager.NumBlocks ) {
					throw new DataInconsistencyException ("Invalid database size.");
				}

				if (NumAllocatedBlocks < 0 ||
					NumAllocatedBlocks > pager.NumBlocks ) {
					throw new DataInconsistencyException ("Invalid allocated block count.");
				}
			} else {
				// header not found. maybe new file?
				RootFreemapBlock = 0;
				DatabaseSize = 8;
				pager.NumBlocks = Math.Max (DatabaseSize, pager.NumBlocks);
			}
		}

	}
}


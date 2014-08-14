using System;
using System.IO;

namespace Yavit.StellaDB.LowLevel
{
	class Superblock
	{
		const uint HeaderMagicSignature = 0x810893ff;
		const uint Version = 0x00000001;
		readonly StellaDB.IO.IBlockStorage storage;

		readonly MemoryStream stream;

		InternalUtils.BitConverter bc;

		// TODO: Superblock: use dirty flag to reduce disk access

		public long RootFreemapBlock { get; set; }

		/// <summary>
		/// Number of valid (allocated or not allocated) blocks in the database.
		/// Changing this value might require structure of freemap tree to be changed,
		/// so be careful.
		/// </summary>
		public long DatabaseSize { get; set; }

		public long NumAllocatedBlocks { get; set; }

		public long UserBlockId1 { get; set; }
		public long UserBlockId2 { get; set; }

		public Superblock (StellaDB.IO.IBlockStorage storage)
		{
			this.storage = storage;
			if (storage == null) {
				throw new ArgumentNullException ("storage");
			}

			stream = new MemoryStream (storage.BlockSize);
			stream.SetLength (storage.BlockSize);
			storage.ReadBlock (0, stream.GetBuffer (), 0);

			var br = new BinaryReader (stream);
			stream.Position = 0;

			bc = new InternalUtils.BitConverter (stream.GetBuffer ());

			uint magic = br.ReadUInt32 ();
			if (magic == HeaderMagicSignature) {
				// header found.
				uint version = br.ReadUInt32 ();
				if (version != Version) {
					throw new InvalidFormatVersionException ("Invalid database version number.");
				}

				int blockSize = br.ReadInt32 ();
				if (blockSize != storage.BlockSize) {
					throw new DataInconsistencyException ("Block size mismatch.");
				}

				RootFreemapBlock = br.ReadInt64 ();
				if (RootFreemapBlock >= storage.NumBlocks ||
					RootFreemapBlock <= 0) {
					throw new DataInconsistencyException ("Invalid root freemap block index.");
				}

				DatabaseSize = br.ReadInt64 ();
				if (DatabaseSize < 0 ||
					DatabaseSize > storage.NumBlocks) {
					throw new DataInconsistencyException ("Invalid database size.");
				}

				NumAllocatedBlocks = br.ReadInt64 ();
				if (NumAllocatedBlocks < 0 ||
					NumAllocatedBlocks > storage.NumBlocks) {
					throw new DataInconsistencyException ("Invalid allocated block count.");
				}

				UserBlockId1 = br.ReadInt64 ();
				UserBlockId2 = br.ReadInt64 ();
			} else {
				// header not found. maybe new file?
				RootFreemapBlock = 0;
				DatabaseSize = 8;
				storage.NumBlocks = Math.Max (DatabaseSize, storage.NumBlocks);
			}
		}



		public void Write()
		{
			var wb = new BinaryWriter (stream);
			stream.Position = 0;

			wb.Write (HeaderMagicSignature);
			wb.Write (Version);
			wb.Write ((int)storage.BlockSize);
			wb.Write (RootFreemapBlock);
			wb.Write (DatabaseSize);
			wb.Write (NumAllocatedBlocks);
			wb.Write (UserBlockId1);
			wb.Write (UserBlockId2);

			storage.WriteBlock (0, stream.GetBuffer (), 0);
		}
	}
}


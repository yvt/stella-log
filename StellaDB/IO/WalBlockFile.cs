using System;
using System.Collections.Generic;
using System.IO;

namespace Yavit.StellaDB.IO
{
	public class WalBlockFile: IBlockStorage
	{
		private readonly BlockFile baseBlockFile;
		private readonly Stream stream;
		private readonly int blockSize;
		private readonly int blockSizeBits;

		private long numBlocks;
		private uint logSequenceNumber;
		private bool needsCheckpoint = false;

		// set when IO error occured
		private Exception lastError = null;

		private const uint HeaderLogMagic = 0x1145148a;

		private sealed class Block
		{
			public long blockId;
			public byte[] originalBytes;
			public byte[] bytes;
		}

		private readonly Dictionary<long, Block> blocks = new Dictionary<long, Block>();

		private readonly MemoryStream tmpstr = new MemoryStream(1024 * 1024);

		public WalBlockFile (BlockFile blockFile, Stream walStream)
		{
			if (blockFile == null)
				throw new ArgumentNullException ("blockFile");
			baseBlockFile = blockFile;
			stream = walStream;
			blockSize = baseBlockFile.BlockSize;

			if (blockSize < 1) {
				throw new InvalidOperationException ("Block size must be positive.");
			}

			if (!InternalUtils.IsPowerOfTwo(blockSize)) {
				throw new InvalidOperationException ("Block size must be a power of two.");
			}

			// compute bit count of block size
			blockSizeBits = InternalUtils.GetBitWidth ((uint)blockSize) - 1;

			numBlocks = blockFile.NumBlocks;

			// replay WAL log
			ReplayLog ();

			// start new sequence
			logSequenceNumber = InternalUtils.GenerateCryptographicRandomNumber ();

			// ... and start new WAL log
			ResetLog ();
		}

		private void ReplayLog() {
			int unit = blockSize >> 5;

			byte[] blockBuffer = new byte[blockSize];

			var br = new BinaryReader (stream);
			stream.Seek (0, SeekOrigin.Begin);

			var tmpbr = new BinaryReader (tmpstr);

			try {
				// check log magic
				uint magic = br.ReadUInt32 ();
				if (magic != HeaderLogMagic) {
					// not a log file.
					return;
				}

				// read sequence number
				logSequenceNumber = br.ReadUInt32();
			} catch (EndOfStreamException) {
				// invalid log file.
				return;
			}

			long startOffset = stream.Position;

			// find the last valid entry (or the first invalid entry / EOF)
			while (stream.Position < stream.Length) {
				long origOffset = stream.Position;
				try {
					++logSequenceNumber;

					// read sequence number
					uint entSeq = br.ReadUInt32();
					if (entSeq != logSequenceNumber) {
						throw new InvalidDataException();
					}

					// read entry size
					ushort entSize = br.ReadUInt16();
					if (entSize < 12) { // should contain block Id and update map
						throw new InvalidDataException();
					}

					// read entry CRC
					uint entCrc = br.ReadUInt32();

					// read entry payload
					tmpstr.SetLength(entSize);
					if (br.Read(tmpstr.GetBuffer(), 0, entSize) < entSize) {
						throw new InvalidDataException();
					}

					// another size
					ushort entSize2 = br.ReadUInt16();
					if (entSize2 != entSize) {
						throw new InvalidDataException();
					}

					// check CRC
					uint actualCrc = InternalUtils.ComputeCrc32(tmpstr.GetBuffer(), 0, entSize);

					if (actualCrc != entCrc) {
						throw new InvalidDataException();
					}

				} catch (EndOfStreamException) {
					stream.Position = origOffset;
					break;
				} catch (OverflowException) {
					stream.Position = origOffset;
					break;
				} catch (InvalidDataException) {
					stream.Position = origOffset;
					break;
				}
			}

			// now stream cursor is at the first invalid entry or EOF.
			// start revert process.
			while (stream.Position > startOffset) {

				// read previous entry size
				stream.Seek (-2, SeekOrigin.Current);
				ushort entSize = br.ReadUInt16 ();

				stream.Seek (-((int)entSize + 2), SeekOrigin.Current);

				// read entry payload
				tmpstr.SetLength(entSize);
				tmpstr.Position = 0;
				if (br.Read(tmpstr.GetBuffer(), 0, entSize) < entSize) {
					throw new System.IO.IOException("Cannot read WAL log entry.");
				}

				// set cursor to previous entry
				stream.Seek (-((int)entSize + 10), SeekOrigin.Current);

				// read payload contents.
				// read block id
				long blockId = tmpbr.ReadInt64 ();
				if (blockId < 0) {
					throw new DataInconsistencyException ("WAL log is corrupted (got negative block ID). Database might be corrupted.");
				}

				// read update bitmap
				uint updateBitmap = tmpbr.ReadUInt32 ();
				if (updateBitmap == 0) {
					throw new DataInconsistencyException ("WAL log is corrupted (update bitmap is empty). Database might be corrupted.");
				}

				// revert block file
				if (updateBitmap != 0xffffffffU) {
					baseBlockFile.ReadBlock (blockId, blockBuffer, 0);
				}

				for (int i = 0; i < 32; ++i) {
					if ((updateBitmap & (1U << i)) == 0) {
						continue;
					}

					if(tmpbr.Read (blockBuffer, i * unit, unit) < unit) {
						throw new DataInconsistencyException ("WAL log is corrupted (entry truncated). Database might be corrupted.");
					}
				}

				baseBlockFile.WriteBlock (blockId, blockBuffer, 0);
			}

			baseBlockFile.Flush ();

			// now block file is at the last checkpoint state.
		}

		private void ResetLog() {
			stream.SetLength(0); // truncate
			stream.Seek (0, SeekOrigin.Begin);
			var wb = new BinaryWriter (stream);
			wb.Write (HeaderLogMagic);
			wb.Write (logSequenceNumber);
			stream.Flush ();
		}

		private bool WriteBlock(Stream stream, Block block) {
			// compute update bitmap
			uint updateBitmap = 0;
			int unit = blockSize >> 5;
			{
				var orig = block.originalBytes;
				var bytes = block.bytes;
				for (int i = 0; i < 32; ++i) {
					for (int j = unit, index = unit * i; j != 0; --j, ++index) {
						if (orig[index] != bytes[index]) {
							updateBitmap |= 1U << i;
							break;
						}
					}
				}
			}

			if (updateBitmap == 0) {
				// no modification
				return false;
			}

			// build log entry
			tmpstr.SetLength(0);
			tmpstr.Position = 0;
			var wb = new System.IO.BinaryWriter (tmpstr);

			++logSequenceNumber;

			// 0 --
			wb.Write (logSequenceNumber);

			// 4 --
			wb.Write ((ushort)0); // placeholder for the entry size

			// 6 --
			wb.Write ((uint)0); // placeholder for CRC32

			// 10 --
			wb.Write (block.blockId);
			wb.Write (updateBitmap);

			// 22 --

			for (int i = 0; i < 32; ++i) {
				if ((updateBitmap & (1U << i)) == 0) {
					continue;
				}

				int start = i * unit;

				wb.Write (block.originalBytes, start, unit);
			}

			// ?? --
			wb.Write ((ushort)0); // placeholder for the entry size

			byte[] buf = tmpstr.GetBuffer ();
			int buflen = (int)tmpstr.Length;

			// compute checksum
			uint sum = InternalUtils.ComputeCrc32 (buf, 10, buflen - 12);
			tmpstr.Position = 6;
			wb.Write (sum);

			// write length
			tmpstr.Position = 4;
			wb.Write (checked((ushort)(buflen - 12)));
			tmpstr.Position = buflen - 2;
			wb.Write (checked((ushort)(buflen - 12)));

			// write entry to log.
			stream.Write (buf, 0, buflen);

			return true;
		}

		public void Flush ()
		{
			if (lastError != null) {
				return;
			}
			lock (blocks) {
				WriteToDisk ();

				if (needsCheckpoint) {
					ResetLog ();
					needsCheckpoint = false;
				}
			}
		}

		/// <summary>
		/// Writes pending blocks to disk, but doesn't mark a checkpoint.
		/// </summary>
		public void WriteToDisk() {
			bool hadChanges = false;
			lock (blocks) {
				try{
					// note: block count is not journalled
					if (numBlocks > baseBlockFile.NumBlocks) {
						baseBlockFile.NumBlocks = numBlocks;
					}

					foreach (var block in blocks.Values) {
						if(WriteBlock (stream, block)) {
							needsCheckpoint = true;
							hadChanges = true;
						}
					}

					if (!hadChanges) {
						blocks.Clear ();
						return;
					}

					stream.Flush ();

					foreach (var block in blocks.Values) {
						baseBlockFile.WriteBlock (block.blockId, block.bytes, 0);
					}
					baseBlockFile.Flush ();

					blocks.Clear ();
				} catch (Exception ex) {
					lastError = ex;
				}
			}
		}

		public long NumBlocks {
			get {
				return numBlocks;
			}
			set {
				if (value > 1L << (62 - blockSizeBits)) {
					throw new ArgumentOutOfRangeException ("value");
				}
				if (value > numBlocks) {
					numBlocks = value;

					// currently, block count is not journaled
				}
			}
		}


		public void ReadBlock (long blockId, byte[] buffer, int offset)
		{
			if (blockId > 1L << (62 - blockSizeBits)) {
				throw new ArgumentOutOfRangeException ("blockId");
			}

			Block block;
			lock (blocks) {
				if (blocks.TryGetValue (blockId, out block)) {
					Buffer.BlockCopy (block.bytes, 0, buffer, offset, blockSize);
				} else {
					baseBlockFile.ReadBlock (blockId, buffer, offset);
				}
			}
		}

		public void WriteBlock (long blockId, byte[] buffer, int offset)
		{
			if (blockId > 1L << (62 - blockSizeBits)) {
				throw new ArgumentOutOfRangeException ("blockId");
			}

			lock (blocks) {
				if (lastError != null) {
					throw new IOException ("All writes to the database file is disabled due to an I/O error.",
						lastError);
				}

				Block block;
				if (!blocks.TryGetValue(blockId, out block)) {
					block = new Block ();

					block.blockId = blockId;
					block.originalBytes = new byte[blockSize];
					baseBlockFile.ReadBlock (blockId, block.originalBytes, 0);

					blocks.Add (blockId, block);
				}

				block.bytes = new byte[blockSize];
				Array.Copy (buffer, offset, block.bytes, 0, blockSize);

				// if buffer reaches 1MiB, write them back to the disk.
				if (blocks.Count * blockSize > 1024 * 1024) {
					WriteToDisk ();
				}

				if (lastError != null) {
					throw new IOException ("All writes to the database file is disabled due to an I/O error.",
						lastError);
				}

				numBlocks = Math.Max (numBlocks, blockId + 1);
			}

		
		}

		public int BlockSize {
			get {
				return blockSize;
			}
		}
	}
}


using System;

namespace Yavit.StellaDB.IO
{
	public class BlockFile: BlockStorage
	{
		System.IO.Stream stream;
		readonly int blockSize;
		long numBlocks;

		public BlockFile (System.IO.Stream stream):
		this(stream, 2048)
		{
		}
		public BlockFile (System.IO.Stream stream, int blockSize)
		{
			if (stream == null)
				throw new ArgumentNullException ("stream");
			if (blockSize < 1) {
				throw new ArgumentOutOfRangeException ("blockSize");
			}
			this.stream = stream;
			this.blockSize = blockSize;
			numBlocks = stream.Length / (long)blockSize;
		}

		public override void ReadBlock (long blockId, byte[] buffer, int start)
		{
			if (blockId < 0) {
				throw new ArgumentOutOfRangeException ("blockId");
			}
			stream.Seek (checked(blockId * blockSize), System.IO.SeekOrigin.Begin);
			stream.Read (buffer, start, blockSize);
		}

		public override void WriteBlock (long blockId, byte[] buffer, int start)
		{
			if (blockId < 0) {
				throw new ArgumentOutOfRangeException ("blockId");
			}
			if (blockId > long.MaxValue - 1) {
				throw new ArgumentOutOfRangeException ("blockId");
			}
			stream.Seek (checked(blockId * blockSize), System.IO.SeekOrigin.Begin);
			stream.Write (buffer, start, blockSize);
			numBlocks = Math.Max (numBlocks, blockId + 1);
		}

		public override void Flush()
		{
			stream.Flush ();
		}

		public override long NumBlocks { 
			get {
				return numBlocks;
			} 
			set {
				if (value > NumBlocks) {
					stream.SetLength (checked(blockSize * value));
					numBlocks = value;
				}
			} 
		}

		public override int BlockSize {
			get {
				return blockSize;
			}
		}
	}
}


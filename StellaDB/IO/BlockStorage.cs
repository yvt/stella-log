using System;

namespace Yavit.StellaDB.IO
{
	public abstract class BlockStorage: MarshalByRefObject
	{
		public abstract int BlockSize { get; }
		public abstract long NumBlocks { get; set; }
		public abstract void ReadBlock( long blockId, byte[] buffer, int start );
		public abstract void WriteBlock( long blockId, byte[] buffer, int start );

		/// <summary>
		/// Ensures changes are made persistent.
		/// </summary>
		public abstract void Flush();
	}
}


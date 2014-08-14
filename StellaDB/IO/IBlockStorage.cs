using System;

namespace Yavit.StellaDB.IO
{
	public interface IBlockStorage
	{
		int BlockSize { get; }
		long NumBlocks { get; set; }
		void ReadBlock( long blockId, byte[] buffer, int start );
		void WriteBlock( long blockId, byte[] buffer, int start );

		/// <summary>
		/// Ensures changes are made persistent.
		/// </summary>
		void Flush();
	}
}


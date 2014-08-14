using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class LowLevelDatabaseTest
	{
		[Test ()]
		public void CreateInstance ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				new StellaDB.LowLevel.LowLevelDatabase (blocks);
			}
		}
		[Test ()]
		public void CreateInstanceAndOpenAgain ()
		{
			using (var tmp = new TemporaryFile ()) {
				using (var stream = tmp.Open ()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					new StellaDB.LowLevel.LowLevelDatabase (blocks);
				}
				using (var stream = tmp.Open ()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					new StellaDB.LowLevel.LowLevelDatabase (blocks);
				}
			}
		}
	}
}


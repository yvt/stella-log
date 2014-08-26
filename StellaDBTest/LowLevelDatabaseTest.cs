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
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				new StellaDB.LowLevel.LowLevelDatabase (blocks);
			}
		}
		[Test, ExpectedException(typeof(ArgumentNullException))]
		public void CreateInstanceNull ()
		{
			new StellaDB.LowLevel.LowLevelDatabase (null);
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


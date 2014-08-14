using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class LowLevelBTreeEntryTest: BaseStreamTest
	{
		void GetTree(Action<LowLevel.BTree> callback)
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				callback (tree);
			}
		}

		void CreateEntry(Action<LowLevel.IKeyValueStoreEntry> callback)
		{
			GetTree (tree => {
				var e = tree.InsertEntry(new byte[] { 114, 51, 4 });
				callback(e);
			});
		}

		protected override void CreateStream (Action<System.IO.Stream> callback)
		{
			CreateEntry (entry => {
				using (var s = entry.OpenValueStream()) {
					callback (s);
				}
			});
		}
	}
}


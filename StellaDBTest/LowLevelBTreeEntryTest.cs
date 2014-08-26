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

		[Test]
		public void CreateAndDeleteEntry()
		{
			GetTree (tree => {
				var data = Utils.GenerateRandomBytes(1024 * 64);
				var e = tree.InsertEntry(new byte[] {1, 2, 3});
				e.WriteValue(data);

				var data2 = e.ReadValue();
				Assert.That(data2, Is.EqualTo(data));

				tree.DeleteEntry(new byte[] {1, 2, 3});

				Assert.That(tree.FindEntry(new byte[] {1, 2, 3}), Is.Null);
			});
		}
		[Test]
		public void CreateAndDeleteEntryDisactivated()
		{
			GetTree (tree => {
				var data = Utils.GenerateRandomBytes(1024 * 64);
				var e = tree.InsertEntry(new byte[] {1, 2, 3});

				// Make e inactive
				tree.InsertEntry(new byte[] {4, 5, 6});

				e.WriteValue(data);

				// Make e inactive
				tree.InsertEntry(new byte[] {7, 8, 9});

				var data2 = e.ReadValue();
				Assert.That(data2, Is.EqualTo(data));

				tree.DeleteEntry(new byte[] {1, 2, 3});

				Assert.That(tree.FindEntry(new byte[] {1, 2, 3}), Is.Null);
			});
		}
	}
}


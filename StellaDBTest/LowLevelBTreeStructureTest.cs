using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class LowLevelBTreeStructureTest
	{
		[Test ()]
		public void Create ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				db.CreateBTree ();
				db.Flush ();
			}
		}
		[Test ()]
		public void AddKeyOnly1 ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				tree.InsertEntry (new byte[] { 114, 51, 4 });
				db.Flush ();
			}
		}
		[Test ()]
		public void AddKeyOnly2 ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var b = new byte[64];
				for (int i = 0; i < 1000; ++i) {
					tree.InsertEntry (b);
				}
				db.Flush ();
			}
		}

		[Test ()]
		public void AddKeyOnly3_1 ()
		{
			AddKeyOnly3 (1);
		}
		[Test ()]
		public void AddKeyOnly3_10 ()
		{
			AddKeyOnly3 (10);
		}
		[Test ()]
		public void AddKeyOnly3_100 ()
		{
			AddKeyOnly3 (100);
		}
		[Test ()]
		public void AddKeyOnly3_1000 ()
		{
			AddKeyOnly3 (1000);
		}
		[Test ()]
		public void AddKeyOnly3_10000 ()
		{
			AddKeyOnly3 (10000);
		}
		public void AddKeyOnly3 (int count)
		{
			using (var tmp = new TemporaryFile ())
			using (var stream = tmp.Open ()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var sw = new System.Diagnostics.Stopwatch ();
				sw.Start ();
				try {
					var b = new byte[4];

					for (int i = 0; i < count; ++i) {
						for (int j = 0; j < 4; ++j) {
							++b [j];
							if (b [j] != 0) {
								break;
							} 
						}
						tree.InsertEntry (b);
					}
					Console.Error.WriteLine ("Insertion Done: {0}ms", sw.ElapsedMilliseconds);
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception! Here's tree dump:");
					try {
						tree.Dump (Console.Error);
					} catch {
					}
					throw;
				}
			}
		}

		[Test ()]
		public void AddKeyOnly4_1 ()
		{
			AddKeyOnly4 (1);
		}
		[Test ()]
		public void AddKeyOnly4_10 ()
		{
			AddKeyOnly4 (10);
		}
		[Test ()]
		public void AddKeyOnly4_100 ()
		{
			AddKeyOnly4 (100);
		}
		[Test ()]
		public void AddKeyOnly4_1000 ()
		{
			AddKeyOnly4 (1000);
		}
		[Test ()]
		public void AddKeyOnly4_10000 ()
		{
			AddKeyOnly4 (10000);
		}
		private void AddKeyOnly4 (int count)
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var sw = new System.Diagnostics.Stopwatch ();
				sw.Start ();
				try {
					var b = new byte[4];

					for (int i = 0; i < 10000; ++i) {
						b = Utils.GenerateRandomBytes(16);
						tree.InsertEntry (b);
					}
					Console.Error.WriteLine("Insertion Done: {0}ms", sw.ElapsedMilliseconds);
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception! Here's tree dump:");
					try {tree.Dump (Console.Error);}catch {}
					throw;
				}
			}
		}
		[Test ()]
		public void AddAndRemove1 ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var key = new byte[] { 114, 51, 4 };
				tree.InsertEntry (key);
				Assert.That (tree.DeleteEntry(key));
				Assert.That (!tree.DeleteEntry(key));
				db.Flush ();
			}
		}
		[Test ()]
		public void AddAndRemove2_1 ()
		{
			AddAndRemove2 (1);
		}
		[Test ()]
		public void AddAndRemove2_10 ()
		{
			AddAndRemove2 (10);
		}
		[Test ()]
		public void AddAndRemove2_100 ()
		{
			AddAndRemove2 (100);
		}
		[Test ()]
		public void AddAndRemove2_1000 ()
		{
			AddAndRemove2 (1000);
		}
		[Test ()]
		public void AddAndRemove2_10000 ()
		{
			AddAndRemove2 (10000);
		}
		[Test ()]
		public void AddAndRemove3_1 ()
		{
			AddAndRemove3 (1);
		}
		[Test ()]
		public void AddAndRemove3_10 ()
		{
			AddAndRemove3 (10);
		}
		[Test ()]
		public void AddAndRemove3_100 ()
		{
			AddAndRemove3 (100);
		}
		[Test ()]
		public void AddAndRemove3_1000 ()
		{
			AddAndRemove3 (1000);
		}
		[Test ()]
		public void AddAndRemove3_10000 ()
		{
			AddAndRemove3 (10000);
		}
		[Test ()]
		public void AddAndRemove4_1 ()
		{
			AddAndRemove4 (1);
		}
		[Test ()]
		public void AddAndRemove4_10 ()
		{
			AddAndRemove4 (10);
		}
		[Test ()]
		public void AddAndRemove4_100 ()
		{
			AddAndRemove4 (100);
		}
		[Test ()]
		public void AddAndRemove4_1000 ()
		{
			AddAndRemove4 (1000);
		}
		[Test ()]
		public void AddAndRemove4_10000 ()
		{
			AddAndRemove4 (10000);
		}

		private void AddAndRemove2 (int count)
		{
			var keys = new List<byte[]> ();

			byte[] b = new byte[4];
			for (int i = 0; i < count; ++i) {
				for (int j = 0; j < 4; ++j) {
					++b [j];
					if (b [j] != 0) {
						break;
					} 
				}
				keys.Add((byte[])b.Clone());
			}

			AddAndRemove (keys);
		}
		private void AddAndRemove3 (int count)
		{
			var keys = new Dictionary<int, byte[]> ();
			var r = new Random (2);

			for (int i = 0; i < count; ++i) {
				byte[] array = new byte[16];
				r.NextBytes (array);
				keys.Add (array[0] + array[1] * 256 + array[2] * 65536 + array[3] * 0x1000000, array);
			}

			AddAndRemove (keys.Values);
		}
		private void AddAndRemove4 (int count)
		{
			var keys = new List<byte[]> ();

			byte[] b = new byte[4];
			for (int i = 0; i < count; ++i) {
				for (int j = 3; j >= 0; --j) {
					++b [j];
					if (b [j] != 0) {
						break;
					} 
				}
				keys.Add((byte[])b.Clone());
			}

			AddAndRemove (keys);
		}
		private void AddAndRemove (IEnumerable<byte[]> keys, int blockSize = 2048)
		{
			using (var tmp = new TemporaryFile ())
			using (var stream = tmp.Open ()) {
				var blocks = new StellaDB.IO.BlockFile (stream, blockSize);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var keyList = keys.ToArray ();

				var prevAllocs = db.NumAllocatedBlocks;

				try {
					foreach (var key in keys) {
						tree.InsertEntry (key);
					}
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception during creating entry! Here's tree dump:");
					try {
						tree.Dump (Console.Error);
					} catch { }
					throw;
				}

				try {
					Assert.That(!tree.DeleteEntry (new byte[]{10})); // non-existent
					for(int i = 0; i < keyList.Length; ++i) {
						/*if (i == 957) {
							Console.Error.WriteLine(" ------- 957 BEFORE -------");
							tree.Dump (Console.Error);
						}
						for (int j = i; j < keyList.Length; ++j) {
							Assert.IsNotNull(tree.FindEntry(keyList[j]));
						}*/
						Assert.That(tree.DeleteEntry (keyList[i]));
						/*if (i == 957) {
							Console.Error.WriteLine(" ------- 957 AFTER -------");
							tree.Dump (Console.Error);
						}*/
					}
					foreach (var key in keys) {
						Assert.That(!tree.DeleteEntry (key));
					}
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception during removing entry! Here's tree dump:");
					try {
						tree.Dump (Console.Error);
					} catch { }
					throw;
				}

				Assert.LessOrEqual (db.NumAllocatedBlocks, prevAllocs + 3, "Allocated blocks were not reclaimed");
			}
		}

		[Test ()]
		public void AddAndRemoveAndAdd ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				var key = new byte[] { 114, 51, 4 };
				tree.InsertEntry (key);
				Assert.That (tree.DeleteEntry(key));
				Assert.That (!tree.DeleteEntry(key));
				tree.InsertEntry (key);
				Assert.That (tree.DeleteEntry(key));
				Assert.That (!tree.DeleteEntry(key));
				db.Flush ();
			}
		}

		[Test]
		public void AddAndRemoveQueued1 ()
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator1(10000));
		}
		[Test]
		public void AddAndRemoveQueued2 ()
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator2(10000));
		}
		[Test]
		public void AddAndRemoveQueued3 ()
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator3(10000));
		}
		private static IEnumerable<byte[]> AddAndRemoveQueuedEnumerator1(int count)
		{
			byte[] b = new byte[4];
			for (int i = 0; i < count; ++i) {
				for (int j = 3; j >= 0; --j) {
					++b [j];
					if (b [j] != 0) {
						break;
					} 
				}
				yield return (byte[])b.Clone ();
			}
		}
		private static IEnumerable<byte[]> AddAndRemoveQueuedEnumerator2(int count)
		{
			byte[] b = new byte[4];
			for (int i = 0; i < count; ++i) {
				for (int j = 0; j < 4; ++j) {
					++b [j];
					if (b [j] != 0) {
						break;
					} 
				}
				yield return (byte[])b.Clone ();
			}
		}
		private static IEnumerable<byte[]> AddAndRemoveQueuedEnumerator3(int count)
		{
			byte[] b = new byte[16];
			var r = new Random (2);
			for (int i = 0; i < count; ++i) {
				r.NextBytes (b);
				yield return (byte[])b.Clone ();
			}
		}

		private void AddAndRemoveQueued (IEnumerable<byte[]> keys)
		{
			using (var tmp = new TemporaryFile ())
			using (var stream = tmp.Open ()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();

				var queue = new Queue<byte[]> (1000);

				try {
					foreach (var key in keys) {
						tree.InsertEntry (key);
						queue.Enqueue(key);
						if (queue.Count > 900) {
							var oldkey = queue.Dequeue();
							Assert.That(tree.DeleteEntry(oldkey));
						}
					}
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception during creating entry! Here's tree dump:");
					try {
						tree.Dump (Console.Error);
					} catch { }
					throw;
				}
			}
		}

		[Test] public void AddAndDrop_0 () { AddAndDrop (0); }
		[Test] public void AddAndDrop_1 () { AddAndDrop (1); }
		[Test] public void AddAndDrop_10 () { AddAndDrop (10); }
		[Test] public void AddAndDrop_100 () { AddAndDrop (100); }
		[Test] public void AddAndDrop_1000 () { AddAndDrop (1000); }
		[Test] public void AddAndDrop_10000 () { AddAndDrop (10000); }

		private void AddAndDrop (int count)
		{
			var keys = new Dictionary<int, byte[]> ();
			var r = new Random (2);

			for (int i = 0; i < count; ++i) {
				byte[] array = new byte[16];
				r.NextBytes (array);
				keys.Add (array[0] + array[1] * 256 + array[2] * 65536 + array[3] * 0x1000000, array);
			}

			AddAndDrop (keys.Values);
		}

		private void AddAndDrop (IEnumerable<byte[]> keys)
		{
			using (var tmp = new TemporaryFile ())
			using (var stream = tmp.Open ()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var prevAllocs = db.NumAllocatedBlocks;
				var tree = db.CreateBTree ();

				try {
					foreach (var key in keys) {
						tree.InsertEntry (key);
					}
					db.Flush ();
				} catch {
					Console.Error.WriteLine ("Exception during creating entry! Here's tree dump:");
					try {
						tree.Dump (Console.Error);
					} catch { }
					throw;
				}

				tree.Drop ();

				Assert.LessOrEqual (db.NumAllocatedBlocks, prevAllocs, "Allocated blocks were not reclaimed");
			}
		}

	}
}


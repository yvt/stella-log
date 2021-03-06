﻿using NUnit.Framework;
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
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				db.CreateBTree ();
				db.Flush ();
			}
		}
		[Test ()]
		public void CreateAndOpen ()
		{
			using (var tmp = new TemporaryFile()) {
				long bId;
				using (var stream = tmp.Open()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
					bId = db.CreateBTree ().BlockId;
					db.Flush ();
				}
				using (var stream = tmp.Open()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
					db.OpenBTree (bId);
				}

			}
		}
		[Test, ExpectedException(typeof(ArgumentOutOfRangeException))]
		public void CreateBadKeyLen ()
		{
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var param = new StellaDB.LowLevel.BTreeParameters ();
				param.MaximumKeyLength = -1;
				db.CreateBTree (param);
			}
		}
		[Test, ExpectedException()]
		public void CreateTooLongKeyLen ()
		{
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var param = new StellaDB.LowLevel.BTreeParameters ();
				param.MaximumKeyLength = 1024 * 114514;
				db.CreateBTree (param);
			}
		}
		[Test, ExpectedException(typeof(ArgumentNullException))]
		public void CreateBadParam ()
		{
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				db.CreateBTree (null);
			}
		}
		[Test, ExpectedException]
		public void DoubleDrop ()
		{
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				tree.Drop ();
				tree.Drop ();
			}
		}
		[Test, ExpectedException]
		public void AddOnDroppedTree ()
		{
			using (var stream = new System.IO.MemoryStream()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var tree = db.CreateBTree ();
				tree.Drop ();
				tree.InsertEntry (new byte[] { 114, 51, 4 });
				db.Flush ();
			}
		}
		[Test ()]
		public void AddKeyOnly1 ()
		{
			using (var stream = new System.IO.MemoryStream()) {
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
			using (var stream = new System.IO.MemoryStream()) {
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

		public void AddKeyOnly3 ([Values(1, 10, 100, 1000, 10000)] int count)
		{
			using (var stream = new System.IO.MemoryStream()) {
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

		private void AddKeyOnly4 ([Values(1, 10, 100, 1000, 10000)] int count)
		{
			using (var stream = new System.IO.MemoryStream()) {
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
			using (var stream = new System.IO.MemoryStream()) {
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

		[Test]
		public void AddAndRemove2 ([Values(0, 1, 10, 100, 1000, 10000)] int count)
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

		[Test]
		public void AddAndRemove3 ([Values(0, 1, 10, 100, 1000, 10000)] int count)
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

		[Test]
		public void AddAndRemove4 ([Values(0, 1, 10, 100, 1000, 10000)] int count)
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
			using (var stream = new System.IO.MemoryStream()) {
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
						Assert.That(tree.DeleteEntry (keyList[i]));
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
			using (var stream = new System.IO.MemoryStream()) {
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
		[Test ()]
		public void AddAndReloadAndRemoveAndAdd ()
		{
			long bId;
			using (var tmp = new TemporaryFile()) {
				var key = new byte[] { 114, 51, 4 };
				using (var stream = tmp.Open()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
					var tree = db.CreateBTree ();
					tree.InsertEntry (key);
					bId = tree.BlockId;
					db.Flush ();

				}
				using (var stream = tmp.Open ()) {
					var blocks = new StellaDB.IO.BlockFile (stream);
					var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
					var tree = db.OpenBTree (bId);
					Assert.That (tree.DeleteEntry(key));
					Assert.That (!tree.DeleteEntry(key));
					tree.InsertEntry (key);
					Assert.That (tree.DeleteEntry(key));
					Assert.That (!tree.DeleteEntry(key));
					db.Flush ();
				}
			}
		}

		[Test]
		public void AddAndRemoveQueued1 ([Values(1, 10, 100, 1000, 10000)] int count)
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator1(count));
		}
		[Test]
		public void AddAndRemoveQueued2 ([Values(1, 10, 100, 1000, 10000)] int count)
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator2(count));
		}
		[Test]
		public void AddAndRemoveQueued3 ([Values(1, 10, 100, 1000, 10000)] int count)
		{
			AddAndRemove (AddAndRemoveQueuedEnumerator3(count));
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
			using (var stream = new System.IO.MemoryStream()) {
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

		[Test]
		public void AddAndDrop ([Values(0, 1, 10, 100, 1000, 10000)]int count)
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
			using (var stream = new System.IO.MemoryStream()) {
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


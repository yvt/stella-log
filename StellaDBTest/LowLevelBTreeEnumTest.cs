using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class LowLevelBTreeEnumTest
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

		void AddAndEnumAscending(IEnumerable<byte[]> keys)
		{
			GetTree (tree => {
				int count1 = 0;
				foreach (var key in keys) {
					tree.InsertEntry(key);
					++count1;
				}
				byte[] lastKey = null;
				int count2 = 0;
				var comparer = new DefaultKeyComparer();
				foreach(var item in tree.EnumerateEntiresInAscendingOrder()) {
					var key = item.GetKey();
					if (lastKey != null) {
						Assert.That(comparer.Compare(key, 0, key.Length, lastKey, 0, lastKey.Length),
							Is.GreaterThan(0));
					}
					lastKey = key;
					++count2;
				}
				Assert.That(count1, Is.EqualTo(count2));
			});
		}
		void AddAndEnumDescending(IEnumerable<byte[]> keys)
		{
			GetTree (tree => {
				int count1 = 0;
				foreach (var key in keys) {
					tree.InsertEntry(key);
					++count1;
				}
				byte[] lastKey = null;
				int count2 = 0;
				var comparer = new DefaultKeyComparer();
				foreach(var item in tree.EnumerateEntiresInDescendingOrder()) {
					var key = item.GetKey();
					if (lastKey != null) {
						Assert.That(comparer.Compare(key, 0, key.Length, lastKey, 0, lastKey.Length),
							Is.LessThan(0));
					}
					lastKey = key;
					++count2;
				}
				Assert.That(count1, Is.EqualTo(count2));
			});
		}


		private static IEnumerable<byte[]> TestEnumerator1(int count)
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
		private static IEnumerable<byte[]> TestEnumerator2(int count)
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
		private static IEnumerable<byte[]> TestEnumerator3(int count)
		{
			byte[] b = new byte[16];
			var r = new Random (2);
			for (int i = 0; i < count; ++i) {
				r.NextBytes (b);
				yield return (byte[])b.Clone ();
			}
		}

		[Test] public void AddAndEnumAscending1([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumAscending (TestEnumerator1 (count)); }
		[Test] public void AddAndEnumAscending2([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumAscending (TestEnumerator2 (count)); }
		[Test] public void AddAndEnumAscending3([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumAscending (TestEnumerator3 (count)); }

		[Test] public void AddAndEnumDescending1([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumDescending (TestEnumerator1 (count)); }
		[Test] public void AddAndEnumDescending2([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumDescending (TestEnumerator2 (count)); }
		[Test] public void AddAndEnumDescending3([Values(0, 1, 10, 100, 1000, 10000)]int count)     
		{ AddAndEnumDescending (TestEnumerator3 (count)); }

	}
}


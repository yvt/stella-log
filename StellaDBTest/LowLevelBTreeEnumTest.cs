using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;
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

		[Test] public void AddAndEnumAscending1_0()     { AddAndEnumAscending (TestEnumerator1 (0)); }
		[Test] public void AddAndEnumAscending1_1()     { AddAndEnumAscending (TestEnumerator1 (1)); }
		[Test] public void AddAndEnumAscending1_10()    { AddAndEnumAscending (TestEnumerator1 (10)); }
		[Test] public void AddAndEnumAscending1_100()   { AddAndEnumAscending (TestEnumerator1 (100)); }
		[Test] public void AddAndEnumAscending1_1000()  { AddAndEnumAscending (TestEnumerator1 (1000)); }
		[Test] public void AddAndEnumAscending1_10000() { AddAndEnumAscending (TestEnumerator1 (10000)); }

		[Test] public void AddAndEnumAscending2_0()     { AddAndEnumAscending (TestEnumerator2 (0)); }
		[Test] public void AddAndEnumAscending2_1()     { AddAndEnumAscending (TestEnumerator2 (1)); }
		[Test] public void AddAndEnumAscending2_10()    { AddAndEnumAscending (TestEnumerator2 (10)); }
		[Test] public void AddAndEnumAscending2_100()   { AddAndEnumAscending (TestEnumerator2 (100)); }
		[Test] public void AddAndEnumAscending2_1000()  { AddAndEnumAscending (TestEnumerator2 (1000)); }
		[Test] public void AddAndEnumAscending2_10000() { AddAndEnumAscending (TestEnumerator2 (10000)); }

		[Test] public void AddAndEnumAscending3_0()     { AddAndEnumAscending (TestEnumerator3 (0)); }
		[Test] public void AddAndEnumAscending3_1()     { AddAndEnumAscending (TestEnumerator3 (1)); }
		[Test] public void AddAndEnumAscending3_10()    { AddAndEnumAscending (TestEnumerator3 (10)); }
		[Test] public void AddAndEnumAscending3_100()   { AddAndEnumAscending (TestEnumerator3 (100)); }
		[Test] public void AddAndEnumAscending3_1000()  { AddAndEnumAscending (TestEnumerator3 (1000)); }
		[Test] public void AddAndEnumAscending3_10000() { AddAndEnumAscending (TestEnumerator3 (10000)); }

		[Test] public void AddAndEnumDescending1_0()     { AddAndEnumDescending (TestEnumerator1 (0)); }
		[Test] public void AddAndEnumDescending1_1()     { AddAndEnumDescending (TestEnumerator1 (1)); }
		[Test] public void AddAndEnumDescending1_10()    { AddAndEnumDescending (TestEnumerator1 (10)); }
		[Test] public void AddAndEnumDescending1_100()   { AddAndEnumDescending (TestEnumerator1 (100)); }
		[Test] public void AddAndEnumDescending1_1000()  { AddAndEnumDescending (TestEnumerator1 (1000)); }
		[Test] public void AddAndEnumDescending1_10000() { AddAndEnumDescending (TestEnumerator1 (10000)); }
																	
		[Test] public void AddAndEnumDescending2_0()     { AddAndEnumDescending (TestEnumerator2 (0)); }
		[Test] public void AddAndEnumDescending2_1()     { AddAndEnumDescending (TestEnumerator2 (1)); }
		[Test] public void AddAndEnumDescending2_10()    { AddAndEnumDescending (TestEnumerator2 (10)); }
		[Test] public void AddAndEnumDescending2_100()   { AddAndEnumDescending (TestEnumerator2 (100)); }
		[Test] public void AddAndEnumDescending2_1000()  { AddAndEnumDescending (TestEnumerator2 (1000)); }
		[Test] public void AddAndEnumDescending2_10000() { AddAndEnumDescending (TestEnumerator2 (10000)); }
																	
		[Test] public void AddAndEnumDescending3_0()     { AddAndEnumDescending (TestEnumerator3 (0)); }
		[Test] public void AddAndEnumDescending3_1()     { AddAndEnumDescending (TestEnumerator3 (1)); }
		[Test] public void AddAndEnumDescending3_10()    { AddAndEnumDescending (TestEnumerator3 (10)); }
		[Test] public void AddAndEnumDescending3_100()   { AddAndEnumDescending (TestEnumerator3 (100)); }
		[Test] public void AddAndEnumDescending3_1000()  { AddAndEnumDescending (TestEnumerator3 (1000)); }
		[Test] public void AddAndEnumDescending3_10000() { AddAndEnumDescending (TestEnumerator3 (10000)); }
	}
}


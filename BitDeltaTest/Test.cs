using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.BitDelta.Test
{
	[TestFixture ()]
	public class BitDeltaTest
	{
		DeltaEncoder encoder = new DeltaEncoder();

		public struct TestData
		{
			public readonly string Name;
			public readonly byte[] Bytes;

			public TestData(params byte[] bytes)
			{
				Bytes = bytes;
				Name = "{" + string.Join(",", bytes.Select(b=>b.ToString())) + "}";
			}
			public TestData(string name, params byte[] bytes)
			{
				Name = name;
				Bytes = bytes;
			}
			public override string ToString ()
			{
				return Name;
			}
		}

		public IEnumerable<TestData> Source
		{
			get {
				yield return new TestData ("Empty", new byte[] {});
				yield return new TestData (1);
				yield return new TestData (2);
				yield return new TestData (1, 1);
				yield return new TestData (2, 1);
				yield return new TestData (1, 1, 1);
				yield return new TestData (1, 2, 1);
				yield return new TestData ("Ascending100",
					Enumerable.Range(0, 100).Select(v=>(byte)v).ToArray());
				yield return new TestData ("Ascending100Masked",
					Enumerable.Range(0, 100).Select(v=>(byte)(v&63)).ToArray());
			}
		}

		[Test]
		public void TestCase ([ValueSource("Source")] TestData inX,
			[ValueSource("Source")] TestData inY)
		{
			var x = (byte[])inX.Bytes.Clone();
			var y = (byte[])inY.Bytes.Clone();

			var delta = encoder.Encode (x, y);

			// Verify that input array is not modified
			Assert.That (x, Is.EqualTo (inX.Bytes));
			Assert.That (y, Is.EqualTo (inY.Bytes));

			Console.WriteLine ("Delta encoded to {0} byte(s)", delta.Length);

			var rX = encoder.DecodeX (delta, y);
			var rY = encoder.DecodeY (delta, x);

			// Verify that input array is not modified
			Assert.That (x, Is.EqualTo (inX.Bytes));
			Assert.That (y, Is.EqualTo (inY.Bytes));

			Assert.That (rX, Is.EqualTo (inX.Bytes));
			Assert.That (rY, Is.EqualTo (inY.Bytes));
		}
	}
}


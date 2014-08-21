using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class LowLevelBlobTest: BaseStreamTest
	{
		void CreateBlob(Action<LowLevel.Blob> callback)
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream);
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks);
				var blob = db.CreateBlob();
				callback (blob);
				db.Flush ();
			}
		}

		protected override void CreateStream (Action<System.IO.Stream> callback)
		{
			CreateBlob (b => {
				using (var s = b.OpenStream ()) {
					callback (s);
				}
			});
		}

		[Test]
		public void SimpleReadWrite([Values(0, 1, 100, 10000, 1000000)] int bytes)
		{
			CreateBlob (b => {
				byte[] data = Utils.GenerateRandomBytes(bytes);
				byte[] buf = (byte[])data.Clone();

				b.WriteAllBytes(buf);
				Assert.That(buf, Is.EqualTo(data));

				buf = b.ReadAllBytes();
				Assert.That(buf, Is.EqualTo(data));

			});
		}


		[Test]
		public void Drop([Values(0, 1, 100, 10000, 1000000)] int bytes)
		{
			CreateBlob (b => {
				byte[] data = Utils.GenerateRandomBytes(bytes);
				byte[] buf = (byte[])data.Clone();
				long siz = b.Database.NumAllocatedBlocks;

				b.WriteAllBytes(buf);
				Assert.That(buf, Is.EqualTo(data));

				Assert.That(b.Database.NumAllocatedBlocks, Is.GreaterThanOrEqualTo(siz));

				b.Drop();

				Assert.That(b.Database.NumAllocatedBlocks, Is.LessThan(siz));
			});
		}
	}
}


using NUnit.Framework;
using System;
using NUnit.Framework.SyntaxHelpers;

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
		public void SimpleReadWrite()
		{
			CreateBlob (b => {
				byte[] data = Utils.GenerateRandomBytes(20000);
				byte[] buf = (byte[])data.Clone();

				b.WriteAllBytes(buf);
				Assert.That(buf, Is.EquivalentTo(data));

				buf = b.ReadAllBytes();
				Assert.That(buf, Is.EquivalentTo(data));

			});
		}


		[Test]
		public void Drop()
		{
			CreateBlob (b => {
				byte[] data = Utils.GenerateRandomBytes(200000);
				byte[] buf = (byte[])data.Clone();
				long siz = b.Database.NumAllocatedBlocks;

				b.WriteAllBytes(buf);
				Assert.That(buf, Is.EquivalentTo(data));

				Assert.That(b.Database.NumAllocatedBlocks, Is.GreaterThan(siz));

				b.Drop();

				Assert.That(b.Database.NumAllocatedBlocks, Is.LessThan(siz));
			});
		}
	}
}


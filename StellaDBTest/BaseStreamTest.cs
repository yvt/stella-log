using System;
using System.IO;
using NUnit.Framework;

namespace Yavit.StellaDB.Test
{
	public abstract class BaseStreamTest
	{
		static byte[] testData1 = new byte[] { 114, 54, 4 };
		static byte[] testData2 = Utils.GenerateRandomBytes(50000);
		static byte[] testData3;

		static BaseStreamTest() {
			testData3 = new byte[10000];
			for (int i = 0; i < testData3.Length; ++i) {
				testData3 [i] = (byte)i;
			}
		}

		protected abstract void CreateStream(Action<Stream> callback);

		[Test]
		public void Create() {
			CreateStream (s => { });
		}

		[Test] public void Write1() { Write (testData1); }
		[Test] public void Write2() { Write (testData2); }
		[Test] public void Write3() { Write (testData3); }

		void Write(byte[] d) {
			CreateStream (s => {
				byte[] buf = new byte[d.Length];
				Buffer.BlockCopy(d, 0, buf, 0, d.Length);
				s.Write (buf, 0, buf.Length);
				Assert.That(buf, Is.EqualTo(d));
			});
		}

		[Test] public void WriteAndVerify1() { WriteAndVerify (testData1); }
		[Test] public void WriteAndVerify2() { WriteAndVerify (testData2); }
		[Test] public void WriteAndVerify3() { WriteAndVerify (testData3); }

		void WriteAndVerify(byte[] d) {
			CreateStream (s => {
				byte[] buf = new byte[d.Length];
				Buffer.BlockCopy(d, 0, buf, 0, d.Length);
				s.Write (buf, 0, buf.Length);
				Assert.That(buf, Is.EqualTo(d));

				s.Position = 0;
				Assert.That(s.Read(buf, 0, buf.Length), Is.EqualTo(buf.Length));
				Assert.That(buf, Is.EqualTo(d));
			});
		}

		[Test] public void WriteAndTruncate1() { WriteAndTruncate (testData1); }
		[Test] public void WriteAndTruncate2() { WriteAndTruncate (testData2); }
		[Test] public void WriteAndTruncate3() { WriteAndTruncate (testData3); }

		void WriteAndTruncate(byte[] d) {
			CreateStream (s => {
				byte[] buf = new byte[d.Length];
				Buffer.BlockCopy(d, 0, buf, 0, d.Length);
				s.Write (buf, 0, buf.Length);
				Assert.That(buf, Is.EqualTo(d));

				Assert.That(s.Length, Is.EqualTo(d.Length));

				int newLength = d.Length / 2;
				s.SetLength(newLength);

				Assert.That(s.Length, Is.EqualTo(newLength));

				s.Position = 0;
				Assert.That(s.Read(buf, 0, buf.Length), Is.EqualTo(newLength));
				Assert.That(buf, Is.EqualTo(d));
			});
		}

	}
}


using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class BufferedBlockFileTest
	{
		[Test]
		public void CreateInstance ()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				new IO.BufferedBlockFile(new IO.BlockFile (stream));
			}
		}
		[Test, ExpectedException(typeof(ArgumentNullException))]
		public void CreateInstanceNull ()
		{
			new IO.BufferedBlockFile(null);
		}

		[Test]
		public void ReadAndWriteBlock1()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

				var r = new Random ();
				var blockSize = blockFile.BlockSize;

				var buf1 = new byte[blockSize];
				var buf2 = new byte[blockSize];
				for (int i = 0; i < blockSize; ++i) {
					buf1 [i] = buf2 [i] = (byte)r.Next (256);
				}

				blockFile.WriteBlock (0, buf1, 0);

				buf1 = new byte[blockSize];

				blockFile.ReadBlock (0, buf1, 0);

				for (int i = 0; i < blockSize; ++i) {
					if (buf1[i] != buf2[i]) {
						Assert.Fail ("At offset {0}. Read {1} (expected {2})",
							i, buf1[i], buf2[i]);
					}
				}

			}
		}

		[Test]
		public void ReadAndWriteBlock2()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

				var r = new Random ();
				var blockSize = blockFile.BlockSize;

				var buf1 = new byte[blockSize * 2];
				var buf2 = new byte[blockSize * 2];
				for (int i = 0; i < blockSize * 2; ++i) {
					buf1 [i] = buf2 [i] = (byte)r.Next (256);
				}

				blockFile.WriteBlock (0, buf1, 0);
				blockFile.WriteBlock (1, buf1, blockSize);

				buf1 = new byte[blockSize * 2];

				blockFile.ReadBlock (0, buf1, 0);
				blockFile.ReadBlock (1, buf1, blockSize);

				for (int i = 0; i < blockSize * 2; ++i) {
					if (buf1[i] != buf2[i]) {
						Assert.Fail ("At offset {0}. Read {1} (expected {2})",
							i, buf1[i], buf2[i]);
					}
				}

			}
		}

		[Test]
		public void ReadAndWriteCloseOnceBlock()
		{
			byte[] buf1, buf2;
			int blockSize;
			using (var tmp = new TemporaryFile()) {
				using (var stream = tmp.Open ()) {
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					var r = new Random ();
					blockSize = blockFile.BlockSize;

					buf1 = new byte[blockSize * 2];
					buf2 = new byte[blockSize * 2];
					for (int i = 0; i < blockSize * 2; ++i) {
						buf1 [i] = buf2 [i] = (byte)r.Next (256);
					}

					blockFile.WriteBlock (0, buf1, 0);
					blockFile.WriteBlock (1, buf1, blockSize);

					blockFile.Flush ();

				}

				using (var stream = tmp.Open ()){
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					buf1 = new byte[blockSize * 2];

					blockFile.ReadBlock (0, buf1, 0);
					blockFile.ReadBlock (1, buf1, blockSize);

					for (int i = 0; i < blockSize * 2; ++i) {
						if (buf1 [i] != buf2 [i]) {
							Assert.Fail ("At offset {0}. Read {1} (expected {2})",
								i, buf1 [i], buf2 [i]);
						}
					}

				}
			}
		}


		[Test]
		public void Resize1()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

				blockFile.NumBlocks = 810;

				Assert.AreEqual (810, blockFile.NumBlocks);
			}
		}

		[Test]
		public void Resize2()
		{
			using (var tmp = new TemporaryFile ()) {
				using (var stream = tmp.Open ()) {
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					blockFile.NumBlocks = 810;
				}
				using (var stream = tmp.Open ()) {
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					Assert.AreEqual (810, blockFile.NumBlocks);
				}
			}
		}

		[Test]
		public void AutoResize1()
		{
			using (var tmp = new TemporaryFile())
			using (var stream = tmp.Open()) {
				var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

				var bytes = new byte[blockFile.BlockSize];
				blockFile.WriteBlock (41, bytes, 0);

				Assert.AreEqual (42, blockFile.NumBlocks);
			}
		}

		[Test]
		public void AutoResize2()
		{
			using (var tmp = new TemporaryFile ()) {
				using (var stream = tmp.Open ()) {
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					var bytes = new byte[blockFile.BlockSize];
					blockFile.WriteBlock (41, bytes, 0);
				}
				using (var stream = tmp.Open ()) {
					var blockFile = new IO.BufferedBlockFile(new IO.BlockFile (stream));

					Assert.AreEqual (42, blockFile.NumBlocks);
				}
			}
		}

	}
}


using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture]
	public class WalBlockFileTest
	{
		[Test]
		public void CreateInstance ()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				new IO.WalBlockFile (new IO.BlockFile(stream), walStream);
			}
		}

		[Test]
		public void ReadAndWriteBlock1()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

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
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

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
		public void ReadAndWriteWithFlushBlock1()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

				var r = new Random ();
				var blockSize = blockFile.BlockSize;

				var buf1 = new byte[blockSize];
				var buf2 = new byte[blockSize];
				for (int i = 0; i < blockSize; ++i) {
					buf1 [i] = buf2 [i] = (byte)r.Next (256);
				}

				blockFile.WriteBlock (0, buf1, 0);

				blockFile.Flush ();

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
		public void ReadAndWriteWithFlushBlock2()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

				var r = new Random ();
				var blockSize = blockFile.BlockSize;

				var buf1 = new byte[blockSize * 2];
				var buf2 = new byte[blockSize * 2];
				for (int i = 0; i < blockSize * 2; ++i) {
					buf1 [i] = buf2 [i] = (byte)r.Next (256);
				}

				blockFile.WriteBlock (0, buf1, 0);
				blockFile.WriteBlock (1, buf1, blockSize);

				blockFile.Flush ();

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
		public void ReadAndWriteCloseOnceBlock1()
		{
			byte[] buf1, buf2;
			int blockSize;
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile ()) {
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open ()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile (stream), walStream);

					var r = new Random ();
					blockSize = blockFile.BlockSize;

					buf1 = new byte[blockSize * 2];
					buf2 = new byte[blockSize * 2];
					for (int i = 0; i < blockSize * 2; ++i) {
						buf1 [i] = buf2 [i] = (byte)r.Next (256);
					}

					blockFile.WriteBlock (0, buf1, 0);
					blockFile.WriteBlock (1, buf1, blockSize);

					// checkpoint is created.
					blockFile.Flush ();

				}

				using (var stream = tmp.Open ())
				using (var walStream = wal.Open ()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile (stream), walStream);

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
		public void ReadAndWriteCloseOnceBlock2()
		{
			byte[] buf1, buf2;
			int blockSize;
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile ()) {
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open ()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile (stream), walStream);

					var r = new Random ();
					blockSize = blockFile.BlockSize;

					buf1 = new byte[blockSize * 2];
					buf2 = new byte[blockSize * 2];
					for (int i = 0; i < blockSize * 2; ++i) {
						buf1 [i] = buf2 [i] = (byte)r.Next (256);
					}

					blockFile.WriteBlock (0, buf1, 0);
					blockFile.WriteBlock (1, buf1, blockSize);

					// note that checkpoint is not created here.
					// closing this file means transaction was interrupted.
					blockFile.WriteToDisk ();

				}

				using (var stream = tmp.Open ())
				using (var walStream = wal.Open ()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile (stream), walStream);

					buf1 = new byte[blockSize * 2];

					blockFile.ReadBlock (0, buf1, 0);
					blockFile.ReadBlock (1, buf1, blockSize);

					for (int i = 0; i < blockSize * 2; ++i) {
						// must be zeroed...
						if (buf1 [i] != 0) {
							Assert.Fail ("At offset {0}. Read {1}",
								i, buf1 [i]);
						}
					}

				}
			}
		}


		[Test]
		public void Resize1()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

				blockFile.NumBlocks = 810;

				Assert.AreEqual (810, blockFile.NumBlocks);
			}
		}

		[Test]
		public void Resize2()
		{
			using (var tmp = new TemporaryFile ())
			using (var wal = new TemporaryFile ()) {
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open())  {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

					blockFile.NumBlocks = 810;
					blockFile.Flush ();
				}
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open())  {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

					Assert.AreEqual (810, blockFile.NumBlocks);
				}
			}
		}

		[Test]
		public void AutoResize1()
		{
			using (var tmp = new TemporaryFile())
			using (var wal = new TemporaryFile())
			using (var stream = tmp.Open())
			using (var walStream = wal.Open()) {
				var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

				var bytes = new byte[blockFile.BlockSize];
				blockFile.WriteBlock (41, bytes, 0);

				Assert.AreEqual (42, blockFile.NumBlocks);
			}
		}

		[Test]
		public void AutoResize2()
		{
			using (var tmp = new TemporaryFile ())
			using (var wal = new TemporaryFile ()) {
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

					var bytes = new byte[blockFile.BlockSize];
					blockFile.WriteBlock (41, bytes, 0);
					blockFile.Flush ();
				}
				using (var stream = tmp.Open ())
				using (var walStream = wal.Open()) {
					var blockFile = new IO.WalBlockFile (new IO.BlockFile(stream), walStream);

					Assert.AreEqual (42, blockFile.NumBlocks);
				}
			}
		}

	}
}


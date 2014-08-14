using System;
using Yavit.StellaDB.IO;
using Yavit.StellaDB.Test;

namespace Yavit.StellaDB.Benchmark
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			try { System.IO.File.Delete("/tmp/test.stelladb"); } catch {}
			using (var tmp = new TemporaryFile("/tmp/test.stelladb"))
			using (var stream = tmp.Open()) {
				var blocks = new StellaDB.IO.BlockFile (stream, 512);
				var dbparam = new StellaDB.LowLevel.LowLevelDatabaseParameters ();
				dbparam.NumCachedBlocks =  1024 * 1024 * 64 / blocks.BlockSize;
				var db = new StellaDB.LowLevel.LowLevelDatabase (blocks, dbparam);
				var param = new LowLevel.BTreeParameters ();
				param.MaximumKeyLength = 4;
				var tree = db.CreateBTree (param);
				var sw = new System.Diagnostics.Stopwatch ();
				var b = new byte[4];

				Console.Out.WriteLine ("Records,Elapsed Time [ms]");
				for (int i = 1; i <= 1000000; ++i) {
					for (int j = 3; j >= 0; --j) {
						++b[j];
						if (b[j] != 0) {
							break;
						} 
					}
					tree.InsertEntry (b);//.WriteValue(new byte[] {114});
					if (i % 10000 == 0) {
						if (i == 10000) {
							sw.Start ();
						}
						Console.Out.WriteLine ("{0}, {1}", i - 10000, sw.ElapsedMilliseconds);
					}
				}


				b = new byte[4];

				Console.Out.WriteLine ("Enumerated Records,Elapsed Time [ms]");
				for (int k = 0; k < 1; ++k){
					Console.Out.WriteLine ("{0}", k);
					int i = 0;
					foreach (var e in tree.EnumerateEntiresInAscendingOrder()) {
						++i;
						if (i % 10000 == 0) {
							if (i == 10000) {
								sw.Start ();
							}
							Console.Out.WriteLine ("{0}, {1}", i - 10000, sw.ElapsedMilliseconds);
						}
					}
				}


				Console.Out.WriteLine ("Deleted Records,Elapsed Time [ms]");
				for (int i = 1; i <= 100000; ++i) {
					for (int j = 3; j >= 0; --j) {
						++b[j];
						if (b[j] != 0) {
							break;
						} 
					}
					if (!tree.DeleteEntry (b)) {
						throw new InvalidOperationException ();
					}
					if (i % 10000 == 0) {
						if (i == 10000) {
							sw.Start ();
						}
						Console.Out.WriteLine ("{0}, {1}", i - 10000, sw.ElapsedMilliseconds);
					}
				}

				db.Flush ();
				//tree.Dump (Console.Out);
			}
		}
	}
}

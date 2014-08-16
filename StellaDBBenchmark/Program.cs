using System;
using Yavit.StellaDB.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Yavit.StellaDB.Benchmark
{

	class MainClass
	{


		public static void Main (string[] args)
		{
			if (args.Length == 0) {
				var fs = typeof(MainClass).GetMethods ();
				Console.WriteLine (" List of tests:");
				foreach (var f in fs) {
					if (f.Name.StartsWith("Test"))
						Console.WriteLine (f.Name);
				}
			} else {
				var name = args [0];
				var f = typeof(MainClass).GetMethod (name);
				f.Invoke (null, new object[]{});
			}
		}

		static double Benchmark(Action action)
		{
			var sw = new System.Diagnostics.Stopwatch ();
			long testCount = 1;
			while (true) {
				sw.Reset ();
				sw.Start ();
				for (long i = 0; i < testCount; ++i)
					action ();
				sw.Stop ();
				if (sw.ElapsedMilliseconds < 1000) {
					testCount *= 2;
					continue;
				} else {
					return testCount / ((double)sw.ElapsedTicks / System.Diagnostics.Stopwatch.Frequency);
				}
			}
		}

		public static void TestLowLevel()
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

		public static void TestSton()
		{
			var st = new Ston.StonSerializer ();
			byte[] bytes = null;
			var dic1 = new Dictionary<string, object>();
			var dic2 = new Dictionary<string, string>();
			for (int i = 0; i < 100; ++i) {
				var key = i.ToString ();
				var val = "hoge";
				dic1.Add (key, val);
				dic2.Add (key, val);
			}
			Console.Out.WriteLine ("Serialize Dictionary<string, object>: {0} ops/s", Benchmark(() => {
				bytes = st.Serialize(dic1);
			}));
			Console.Out.WriteLine ("Serialize Dictionary<string, string>: {0} ops/s", Benchmark(() => {
				bytes = st.Serialize(dic2);
			}));

			Console.Out.WriteLine ("DeserializeObject: {0} ops/s", Benchmark(() => {
				st.DeserializeObject(bytes);
			}));
			Console.Out.WriteLine ("Deserialize Dictionary<string, object>: {0} ops/s", Benchmark(() => {
				st.Deserialize<IDictionary<string, object>>(bytes);
			}));
			Console.Out.WriteLine ("Deserialize Dictionary<string, string>: {0} ops/s", Benchmark(() => {
				st.Deserialize<IDictionary<string, string>>(bytes);
			}));

		}


	}

	public class TemporaryFile: IDisposable
	{
		private string fileName;
		public TemporaryFile (): this(Path.GetTempFileName())
		{
		}

		public TemporaryFile (string fileName)
		{
			this.fileName = fileName;
		}

		public string FileName 
		{
			get 
			{
				return fileName;
			}
		}

		public FileStream Open()
		{
			return File.Open (fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite,
				FileShare.Read | FileShare.Delete);
		}

		public void Dispose ()
		{
			if (fileName != null) {
				File.Delete (fileName);
				fileName = null;
			}
		}
	}
}

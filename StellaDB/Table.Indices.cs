using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB
{
	public partial class Table
	{
		sealed class TableIndex
		{
			public readonly Indexer.Index Index;
			public readonly LowLevel.BTree store;
			// TODO
		}

		public class IndexEntry
		{
			public readonly string name;
			public readonly Indexer.KeyParameter keyParameter;

			public string Name {
				get { return name; }
			}

			public Indexer.KeyParameter KeyParameter {
				get { return keyParameter; }
			}

			public IndexEntry(string name, Indexer.KeyParameter param)
			{
				this.name = name;
				this.keyParameter = param;
			}

			public static IndexEntry CreateNumericIndexEntry(string name)
			{
				return new IndexEntry (name, new Indexer.NumericKeyParameters ());
			}
			public static IndexEntry CreateBinrayIndexEntry(string name, int maximumKeyLength)
			{
				return new IndexEntry (name, new Indexer.BinaryKeyParameters() { KeyLength = maximumKeyLength });
			}
		}

		Dictionary<string[], TableIndex> indices = 
			new Dictionary<string[], TableIndex>(Utils.ArrayComparer<string>.Default);

		void LoadIndices()
		{
			indices.Clear ();
			if (store == null) {
				// Table is not materialized
				return;
			}
			// TODO: load indices
		}

		public void EnsureIndex(IndexEntry[] entries)
		{
			if (entries == null)
				throw new ArgumentNullException ("entries");
			if (entries.Length == 0)
				throw new ArgumentException ("No entries given.");
			if (entries.Any (e => e == null))
				throw new ArgumentException ("One of the entries is null.");
			if (entries.Any (e => e.keyParameter == null))
				throw new ArgumentException ("THe key parameter of one of the entries is null.");
			if (entries.Any (e => string.IsNullOrEmpty (e.name)))
				throw new ArgumentException ("THe name of one of the entries is null or empty.");

			EnsureLoaded ();

			// Does index already exist?
			var names = (from entry in entries
				select entry.name).ToArray();
			if (indices.ContainsKey(names)) {
				return;
			}

			// TODO
		}

	}
}


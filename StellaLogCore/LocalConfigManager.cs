﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Yavit.StellaLog.Core
{
	public sealed class LocalConfigManager
	{
		readonly LogBook book;

		readonly StellaDB.Table table;
		readonly Dictionary<string, Entry> entries =
			new Dictionary<string, Entry>();

		readonly StellaDB.PreparedQuery query;
		byte[] queryKey;

		static readonly Encoding utf8 = new UTF8Encoding();

		readonly object sync = new object();

		[Serializable]
		sealed class DbEntry
		{
			public byte[] Key;
			public object Value;
		}

		sealed class Entry
		{
			public long RowId;
			public object Value;
		}

		internal LocalConfigManager (LogBook book)
		{
			this.book = book;

			table = book.database ["LocalConfig"];
			table.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Key", 32)
			});

			query = table.Prepare ((rowId, entry) => entry ["Key"] == queryKey);
		}

		public object this [string key]
		{
			get {
				lock (sync) {
					Entry entry;
					if (entries.TryGetValue (key, out entry)) {
						return entry.Value;
					}

					queryKey = utf8.GetBytes (key);
					lock (book.database) {
						foreach (var result in table.Query(query)) {
							entry = new Entry () {
								RowId = result.RowId,
								Value = result.ToObject<DbEntry> ().Value
							};
							entries.Add (key, entry);
							return entry.Value;
						}
					}

					return null;
				}
			}
			set {
				lock (sync) {
					using (var t = book.BeginTransaction ()) {
						queryKey = utf8.GetBytes (key);
						var obj = new DbEntry () {
							Key = queryKey,
							Value = value
						};

						Entry entry;
						if (entries.TryGetValue (key, out entry)) {
							entry.Value = value;
							table.Update (entry.RowId, obj);
							t.Commit ();
						} else {
							foreach (var result in table.Query(query)) {
								entry = new Entry () {
									RowId = result.RowId,
									Value = value
								};
								entries.Add (key, entry);
								table.Update (entry.RowId, obj);
								t.Commit ();
								return;
							}

							entry = new Entry () {
								RowId = table.Insert (obj, false),
								Value = value
							};
							entries.Add (key, entry);
							t.Commit ();
						}
					}
				}
			}
		}
	}
}


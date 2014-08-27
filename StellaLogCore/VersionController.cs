using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.ComponentModel;

namespace Yavit.StellaLog.Core
{
	public sealed class VersionController
	{
		public const int RevisionIdLength = 24;

		static readonly Encoding utf8 = new UTF8Encoding ();

		readonly LogBook book;
		readonly StellaDB.Database db;

		readonly StellaDB.Table branchTable;
		readonly Func<byte[], StellaDB.Table.PreparedQuery> branchTableLookupQuery;

		readonly StellaDB.Table revisionTable;

		[Serializable]
		struct DbBranch
		{
			public byte[] Name;
			public byte[] Revision;
		}

		[Serializable]
		struct DbRevision
		{
			public byte[] Id;

			public double Timestamp;

			[DefaultValue(null)]
			public IEnumerable<byte[]> Parents;

			[DefaultValue(null)]
			public string Message;

			public DateTime DateTime
			{
				get { return System.DateTime.FromOADate (Timestamp); }
				set { Timestamp = value.ToOADate (); }
			}
		}

		internal VersionController (LogBook book)
		{
			this.book = book;
			db = book.Database;

			branchTable = db["StellaVCS.Branches"];
			revisionTable = db ["StellaVCS.Revisions"];

			branchTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Name", 32)
			});
			revisionTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Id", RevisionIdLength)
			});

			{
				byte[] nameval = null;
				var q = branchTable.Prepare ((rowId, value) => value ["Name"] == nameval);
				branchTableLookupQuery = (name) => {
					nameval = name; return q;
				};
			}

			if (CurrentBranchRaw == null) {
				// Not initialized yet.
				// Create epoch revision.
				var root = GetRootRevisionId ();

				revisionTable.Insert (new DbRevision () {
					Id = root,
					Timestamp = DateTime.UtcNow.ToOADate(),
					Message = "Epoch Revision"
				}, false);

				// Create the first branch.
				var master = utf8.GetBytes ("master");
				branchTable.Insert (new DbBranch () {
					Name = master,
					Revision = root
				}, false);
				CurrentBranchRaw = master;
			}
		}

		internal byte[] CurrentBranchRaw
		{
			get {
				return (byte[])book.LocalConfig ["StellaVCS.Branch"];
			}
			set {
				book.LocalConfig ["StellaVCS.Branch"] = value;
			}
		}

		public string GetCurrentBranch()
		{
			return utf8.GetString (CurrentBranchRaw);
		}

		internal byte[] CurrentRevisionRaw
		{
			get {
				foreach (var row in branchTable.Query(branchTableLookupQuery(CurrentBranchRaw))) {
					var rev = row.ToObject<DbBranch> ();
					return rev.Revision;
				}
				throw new InvalidOperationException ("Current branch " + GetCurrentBranch() + " was not found in the database.");
			}
			set {
				if (value.Length != RevisionIdLength) {
					throw new ArgumentException ("Length of the revision ID is invalid.", "value");
				}
				foreach (var row in branchTable.Query(branchTableLookupQuery(CurrentBranchRaw))) {
					var rev = row.ToObject<DbBranch> ();
					rev.Revision = value;
					branchTable.Update (row.RowId, rev);
				}
				throw new InvalidOperationException ("Current branch " + GetCurrentBranch() + " was not found in the database.");
			}
		}

		public byte[] GetCurrentRevision()
		{
			return (byte[])CurrentRevisionRaw.Clone();
		}

		static readonly RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();

		public byte[] GenerateRevisionId()
		{
			lock (rng) {
				var dt = DateTime.UtcNow;
				byte[] id = new byte[RevisionIdLength];
				rng.GetBytes (id);
				Buffer.BlockCopy (BitConverter.GetBytes (dt.ToOADate ()), 0, id, 0, 8);
				return id;
			}
		}

		public byte[] GetRootRevisionId()
		{
			return new byte[RevisionIdLength];
		}

		public static string RevisionIdToString(byte[] b)
		{
			return "revision:" + Convert.ToBase64String (b);
		}
		public static byte[] RevisionIdFromString(string s)
		{
			if (!s.StartsWith("revision:", StringComparison.InvariantCulture)) {
				throw new FormatException ("Invalid revision id: malformated.");
			}
			byte[] b = Convert.FromBase64String (s.Substring ("revision:".Length));
			if (b.Length != RevisionIdLength) {
				throw new FormatException ("Invalid revision id: bad length.");
			}
			return b;
		}
	}
}


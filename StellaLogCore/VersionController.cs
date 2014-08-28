using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.ComponentModel;
using System.Linq;

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
		readonly Func<byte[], StellaDB.Table.PreparedQuery> revisionTableLookupQuery;

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

			// Reference revision is used for delta encoding
			public byte[] ReferenceRevision
			{
				get { return Parents != null ? Parents.First() : null; }
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
			{
				byte[] nameval = null;
				var q = revisionTable.Prepare ((rowId, value) => value ["Id"] == nameval);
				revisionTableLookupQuery = (name) => {
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

				// Create "detached HEAD" branch.
				branchTable.Insert (new DbBranch () {
					Name = new byte[] {},
					Revision = root
				}, false);

			}
		}

		#region Raw Branch/Revision

		sealed class Revision
		{
			readonly VersionController vc;
			readonly long rowId;
			public DbRevision DbRevision;

			public Revision(VersionController vc, long rowId, DbRevision rev)
			{
				this.vc = vc;
				this.rowId = rowId;
				this.DbRevision = rev;
			}

			public void Save()
			{
				using (var t = vc.book.BeginTransaction ()) {
					vc.revisionTable.Update (rowId, DbRevision);
				}
			}
		}

		sealed class Branch
		{
			readonly VersionController vc;
			readonly long rowId;
			public DbBranch DbBranch;

			public Branch(VersionController vc, long rowId, DbBranch br)
			{
				this.vc = vc;
				this.rowId = rowId;
				this.DbBranch = br;
			}

			public void Save()
			{
				using (var t = vc.book.BeginTransaction ()) {
					vc.branchTable.Update (rowId, DbBranch);
				}
			}
		}

		internal byte[] CurrentBranchRaw
		{
			get {
				return (byte[])book.LocalConfig ["StellaVCS.Branch"];
			}
			set {
				using (var t = book.BeginTransaction ()) {
					book.LocalConfig ["StellaVCS.Branch"] = value;
					t.Commit ();
				}
			}
		}

		internal byte[] CurrentRevisionRaw
		{
			get {
				return LookupBranch (CurrentBranchRaw).DbBranch.Revision;
			}
			set {
				var branch = LookupBranch (CurrentBranchRaw);
				branch.DbBranch.Revision = value;
				branch.Save ();
			}
		}

		Branch LookupBranch(byte[] name)
		{
			foreach (var row in branchTable.Query(branchTableLookupQuery(name))) {
				return new Branch (this, row.RowId, row.ToObject<DbBranch> ());
			}
			throw new InvalidOperationException ("Branch " + utf8.GetString(name) + " was not found in the database.");
		}

		Revision LookupRevision(byte[] name)
		{
			ValidateRevisionId (name);
			foreach (var row in revisionTable.Query(revisionTableLookupQuery(name))) {
				return new Revision (this, row.RowId, row.ToObject<DbRevision> ());
			}
			throw new InvalidOperationException ("Revision " + RevisionIdToString(name) + " was not found in the database.");
		}

		#endregion

		#region Revision Control

		public string GetCurrentBranch()
		{
			return utf8.GetString (CurrentBranchRaw);
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

		static readonly byte[] DetachedBranchId = new byte[] {};

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
			ValidateRevisionId (b);
			return b;
		}

		public static void ValidateRevisionId(byte[] id)
		{
			if (id == null)
				throw new ArgumentNullException ("id");
			if (id.Length != RevisionIdLength) {
				throw new FormatException ("Invalid revision id: bad length.");
			}
		}

		public void SetCurrentBranch(string name)
		{
			if (GetCurrentBranch() == name) {
				return;
			}

			var originalBranchId = CurrentRevisionRaw;
			var goal = LookupBranch (utf8.GetBytes (name));
			var comparer = StellaDB.DefaultKeyComparer.Instance;

			if (comparer.Equals(goal.DbBranch.Revision, originalBranchId)) {
				// Just switch the branch
				CurrentBranchRaw = goal.DbBranch.Name;
				return;
			}

			// First switch to the detached branch.
			CurrentBranchRaw = DetachedBranchId;
			CurrentRevisionRaw = originalBranchId;

			// Check out the target revision.
			SetCurrentRevision (goal.DbBranch.Revision);

			// Switch to the target branch.
			CurrentBranchRaw = goal.DbBranch.Name;
		}

		sealed class CommitTreePath
		{
			public IEnumerable<Revision> CurrentToCommonAncestor;
			public Revision CommonAncestor;
			public IEnumerable<Revision> CommonAncestorToGoal;
		}

		/// <summary>
		/// Checks out the specified revision by moving the current branch's pointer.
		/// </summary>
		/// <param name="revision">Revision.</param>
		public void SetCurrentRevision(byte[] revision)
		{
			var path = ComputeCommitTreePath (CurrentRevisionRaw, revision);
			if (path == null) {
				// No change.
				return;
			}
			foreach (var r in path.CurrentToCommonAncestor) {
				RevertModificationsOfRevision (r);
			}
			foreach (var r in path.CommonAncestorToGoal) {
				ApplyModificationsOfRevision (r);
			}
		}

		CommitTreePath ComputeCommitTreePath(byte[] currentId, byte[] goalId)
		{
			ValidateRevisionId (currentId);
			ValidateRevisionId (goalId);

			var goal = LookupRevision (goalId);
			var current = LookupRevision (currentId);

			// Find the first common ancestor in commit tree
			// (where the parent of each revision is its reference revision).
			var goalAncestors = new List<Revision> ();
			var currentAncestors = new List<Revision> ();
			{
				var goalAncestor = goal;
				var currentAncestor = current;
				var comparator = StellaDB.DefaultKeyComparer.Instance;
				var ancestors = new Dictionary<byte[], Tuple<int, bool>>(comparator);

				if (comparator.Equals(goalId, currentId)) {
					return null;
				}

				goalAncestors.Add (goalAncestor);
				ancestors.Add (goalAncestor.DbRevision.Id, new Tuple<int, bool> (0, true));
				currentAncestors.Add (currentAncestor);
				ancestors.Add (currentAncestor.DbRevision.Id, new Tuple<int, bool> (0, false));

				while (true) {
					bool hasGoalParent = goalAncestor.DbRevision.ReferenceRevision != null;
					bool hasCurParent = currentAncestor.DbRevision.ReferenceRevision != null;
					if (!hasGoalParent && !hasCurParent) {
						throw new InvalidOperationException (string.Format(
							"Failed to find the common ancestor revision of {0} and {1}.",
							RevisionIdToString(current.DbRevision.Id),
							RevisionIdToString(goalId)));
					}
					if (hasGoalParent && hasCurParent) {
						// Choose one using heuristics
						// (Assuming there's no time skew)
						if (currentAncestor.DbRevision.DateTime <
							goalAncestor.DbRevision.DateTime) {
							hasCurParent = false;
						} else {
							hasGoalParent = false;
						}
					}
					Tuple<int, bool> tuple;
					if (hasGoalParent) {
						goalAncestor = LookupRevision (goalAncestor.DbRevision.ReferenceRevision);
						goalAncestors.Add (goalAncestor);
						if (ancestors.TryGetValue(goalAncestor.DbRevision.Id, out tuple)) {
							if (tuple.Item2) {
								throw new InvalidOperationException (string.Format(
									"Circular reference was found while finding the common ancestor revision of {0} and {1}.",
									RevisionIdToString(current.DbRevision.Id),
									RevisionIdToString(goalId)));
							}
							int index = tuple.Item1;
							currentAncestors.RemoveRange (index + 1, currentAncestors.Count - index - 1);
							break;
						} else {
							ancestors.Add (goalAncestor.DbRevision.Id, new Tuple<int, bool> (goalAncestors.Count - 1, true));
						}
					} else if (hasCurParent) {
						currentAncestor = LookupRevision (currentAncestor.DbRevision.ReferenceRevision);
						currentAncestors.Add (currentAncestor);
						if (ancestors.TryGetValue(currentAncestor.DbRevision.Id, out tuple)) {
							if (!tuple.Item2) {
								throw new InvalidOperationException (string.Format(
									"Circular reference was found while finding the common ancestor revision of {0} and {1}.",
									RevisionIdToString(current.DbRevision.Id),
									RevisionIdToString(goalId)));
							}
							int index = tuple.Item1;
							goalAncestors.RemoveRange (index + 1, goalAncestors.Count - index - 1);
							break;
						} else {
							ancestors.Add (currentAncestor.DbRevision.Id, new Tuple<int, bool> (currentAncestors.Count - 1, false));
						}
					} else {
						throw new InvalidOperationException ();
					}
				}
			}
			// Now the last elements of goalAncestors and currentAncestors are
			// the common ancestor.

			var ret = new CommitTreePath ();

			// First, move the current revision to the common ancestor.
			ret.CommonAncestorToGoal = currentAncestors.Take (currentAncestors.Count - 1);

			// And then move the current revision to the goal revision.
			ret.CommonAncestorToGoal = goalAncestors.Take (goalAncestors.Count - 1).Reverse ();

			ret.CommonAncestor = currentAncestors [currentAncestors.Count - 1];

			return ret;
		}

		/// <summary>
		/// Checks out the specified revision.
		/// </summary>
		/// <param name="revision">Revision.</param>
		public void CheckoutRevision(byte[] revision)
		{
			ValidateRevisionId (revision);

			var comparer = StellaDB.DefaultKeyComparer.Instance;
			if (comparer.Equals(revision, CurrentRevisionRaw)) {
				return;
			}

			// Switch to the detached branch
			var originalRevisionId = CurrentRevisionRaw;
			CurrentBranchRaw = DetachedBranchId;

			SetCurrentRevision (revision);
		}

		#endregion

		#region Merge

		public void MergeRevision(byte[] mergedRevision)
		{
			ValidateRevisionId (mergedRevision);

			// 
			var co = ComputeCommitTreePath (CurrentRevisionRaw, mergedRevision);

			throw new NotImplementedException ();
		}

		#endregion

		#region Applying Commits

		void ApplyModificationsOfRevision(Revision r)
		{
			var comparer = StellaDB.DefaultKeyComparer.Instance;
			if (!comparer.Equals(r.DbRevision.ReferenceRevision, CurrentRevisionRaw)) {
				throw new InvalidOperationException (string.Format(
					"Attempted to apply the modification of [{0} -> {1}] when " +
					"the current revision is {2}.",
					RevisionIdToString(r.DbRevision.ReferenceRevision),
					RevisionIdToString(r.DbRevision.Id),
					RevisionIdToString(CurrentRevisionRaw)));
			}

			using (var t = book.BeginTransaction()) {
				throw new NotImplementedException ();

				CurrentRevisionRaw = r.DbRevision.Id;

				t.Commit ();
			}
		}
		void RevertModificationsOfRevision(Revision r)
		{
			if (r.DbRevision.ReferenceRevision == null) {
				throw new InvalidOperationException ("Attempted to revert modification of the root revision.");
			}

			var comparer = StellaDB.DefaultKeyComparer.Instance;
			if (!comparer.Equals(r.DbRevision.Id, CurrentRevisionRaw)) {
				throw new InvalidOperationException (string.Format(
					"Attempted to revert the modification of [{0} -> {1}] when " +
					"the current revision is {2}.",
					RevisionIdToString(r.DbRevision.ReferenceRevision),
					RevisionIdToString(r.DbRevision.Id),
					RevisionIdToString(CurrentRevisionRaw)));
			}

			using (var t = book.BeginTransaction ()) {
				throw new NotImplementedException ();

				CurrentRevisionRaw = r.DbRevision.ReferenceRevision;

				t.Commit ();
			}
		}

		#endregion
	}
}


using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.ComponentModel;
using System.Linq;
using Yavit.StellaLog.Core.Utils;

namespace Yavit.StellaLog.Core
{
	public sealed class CheckoutEventArgs: EventArgs
	{
		internal readonly byte[] oldRevisionId;
		internal readonly byte[] newRevisionId;

		public CheckoutEventArgs(byte[] oldRevisionId, byte[] newRevisionId)
		{
			this.newRevisionId = (byte[])newRevisionId.Clone();
			this.oldRevisionId = (byte[])oldRevisionId.Clone();
		}

		public byte[] GetOldRevisionId()
		{
			return (byte[])oldRevisionId.Clone();
		}
		public byte[] GetNewRevisionId()
		{
			return (byte[])newRevisionId.Clone();
		}
	}

	public sealed class MergingEventArgs: EventArgs
	{
		internal readonly byte[] currentRevisionId;
		internal readonly byte[] sourceRevisionId;

		public MergingEventArgs(byte[] currentRevisionId, byte[] sourceRevisionId)
		{
			this.currentRevisionId = (byte[])currentRevisionId.Clone();
			this.sourceRevisionId = (byte[])sourceRevisionId.Clone();
		}

		public byte[] GetcurrentRevisionId()
		{
			return (byte[])currentRevisionId.Clone();
		}
		public byte[] GetSourceRevisionId()
		{
			return (byte[])sourceRevisionId.Clone();
		}
	}

	public sealed class MergedEventArgs: EventArgs
	{
		internal readonly byte[] oldRevisionId;
		internal readonly byte[] newRevisionId;
		internal readonly byte[] sourceRevisionId;
		readonly VersionController.MergeResult result;

		public MergedEventArgs(byte[] oldRevisionId, byte[] newRevisionId, byte[] sourceRevisionId, VersionController.MergeResult result)
		{
			this.sourceRevisionId = (byte[])sourceRevisionId.Clone();
			this.result = result;
			this.newRevisionId = (byte[])newRevisionId.Clone();
			this.oldRevisionId = (byte[])oldRevisionId.Clone();
		}

		public byte[] GetOldRevisionId()
		{
			return (byte[])oldRevisionId.Clone();
		}
		/// <summary>
		/// Gets the revision identifier which was resulted in by merge operation.
		/// When <c>Result</c> is <c>MergeResult.MergeUnresolved</c>, this is null because
		/// the merge operation is incomplete yet.
		/// </summary>
		/// <returns>The new revision identifier.</returns>
		public byte[] GetNewRevisionId()
		{
			return (byte[])newRevisionId.Clone();
		}

		public byte[] GetSourceRevisionId()
		{
			return (byte[])sourceRevisionId.Clone();
		}

		public VersionController.MergeResult Result
		{
			get { return result; }
		}
	}

	public sealed class CommitEventArgs: EventArgs
	{
		internal readonly byte[] currentRevisionId;
		internal readonly byte[] newRevisionId;

		public CommitEventArgs(byte[] currentRevisionId, byte[] newRevisionId)
		{
			this.currentRevisionId = (byte[])currentRevisionId.Clone();
			this.newRevisionId = (byte[])newRevisionId.Clone();
		}

		public byte[] GetcurrentRevisionId()
		{
			return (byte[])currentRevisionId.Clone();
		}
		public byte[] GetNewRevisionId()
		{
			return (byte[])newRevisionId.Clone();
		}
	}

	public sealed class RevertEventArgs: EventArgs
	{ }

	public sealed class VersionController
	{
		public const int RevisionIdLength = 24;

		static readonly Encoding utf8 = new UTF8Encoding ();

		readonly LogBook book;
		readonly StellaDB.Database db;

		readonly StellaDB.Table branchTable;
		readonly Func<byte[], StellaDB.PreparedQuery> branchTableLookupQuery;

		readonly StellaDB.Table revisionTable;
		readonly Func<byte[], StellaDB.PreparedQuery> revisionTableLookupQuery;

		readonly StellaDB.Table deltaTable;
		readonly Func<byte[], byte[], long, StellaDB.PreparedQuery> deltaTableLookupQuery;
		readonly Func<byte[], StellaDB.PreparedQuery> deltaTableFindByRevisionQuery;

		readonly StellaDB.Table mergeTable;
		readonly StellaDB.PreparedQuery mergeTableEnumerateQuery;
		readonly Func<byte[], long, StellaDB.PreparedQuery> mergeTableLookupQuery;
		readonly Func<byte[], StellaDB.PreparedQuery> mergeTableFindByTableQuery;

		public event EventHandler<CheckoutEventArgs> Checkouting;
		public event EventHandler<CheckoutEventArgs> Checkouted;

		public event EventHandler<MergingEventArgs> Merging;
		public event EventHandler<MergedEventArgs> Merged;

		public event EventHandler<CommitEventArgs> Commiting;
		public event EventHandler<CommitEventArgs> Committed;

		public event EventHandler<RevertEventArgs> Reverting;
		public event EventHandler<RevertEventArgs> Reverted;

		[Serializable]
		sealed class DbBranch
		{
			public byte[] Name;
			public byte[] Revision;
		}

		[Serializable]
		sealed class DbRevision
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

		[Serializable]
		sealed class DbDelta
		{
			public byte[] Revision;
			public byte[] Table;
			public long RowId;
			public byte[] Delta;
		}

		[Serializable]
		sealed class DbMergeItem
		{
			public byte[] Table;
			public long RowId;

			public byte[] Original;
			public byte[] Value1;
			public byte[] Value2;
			public byte[] Revision1;
			public byte[] Revision2;
			public double Time1;
			public double Time2;
		}

		internal VersionController (LogBook book)
		{
			this.book = book;
			db = book.database;

			branchTable = db["StellaVCS.Branches"];
			revisionTable = db ["StellaVCS.Revisions"];
			deltaTable = db ["StellaVCS.Deltas"];
			mergeTable = db ["StellaVCS.Merge"];

			branchTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Name", 32)
			});
			revisionTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Id", RevisionIdLength)
			});
			deltaTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Revision", RevisionIdLength),
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Table", 48),
				StellaDB.Table.IndexEntry.CreateNumericIndexEntry("RowId")
			});
			mergeTable.EnsureIndex (new [] {
				StellaDB.Table.IndexEntry.CreateBinaryIndexEntry("Table", 48),
				StellaDB.Table.IndexEntry.CreateNumericIndexEntry("RowId")
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
			{
				byte[] revisionval = null;
				byte[] tableval = null;
				long rowIdVal = 0;
				var q = deltaTable.Prepare ((rowId, value) => 
					value ["Revision"] == revisionval &&
					value["Table"] == tableval &&
					value["RowId"] == rowIdVal);
				deltaTableLookupQuery = (revision, table, rowId) => {
					revisionval = revision;
					tableval = table;
					rowIdVal = rowId;
					return q;
				};
			}
			{
				byte[] revisionval = null;
				var q = deltaTable.Prepare ((rowId, value) => value ["Revision"] == revisionval);
				deltaTableFindByRevisionQuery = (revision) => {
					revisionval = revision;
					return q;
				};
			}
			{
				byte[] tableval = null;
				long rowIdVal = 0;
				var q = mergeTable.Prepare ((rowId, value) => 
					value["Table"] == tableval &&
					value["RowId"] == rowIdVal);
				mergeTableLookupQuery = (table, rowId) => {
					tableval = table;
					rowIdVal = rowId;
					return q;
				};
			}
			{
				byte[] tableval = null;
				var q = mergeTable.Prepare ((rowId, value) => 
					value["Table"] == tableval);
				mergeTableFindByTableQuery = (table) => {
					tableval = table;
					return q;
				};
			}
			mergeTableEnumerateQuery = mergeTable.Prepare((rowId, value) => true);

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

		protected void OnCheckouting(CheckoutEventArgs e)
		{
			if (Checkouting != null)
				Checkouting (this, e);
		}

		protected void OnCheckouted(CheckoutEventArgs e)
		{
			if (Checkouted != null)
				Checkouted (this, e);
		}

		protected void OnMerging(MergingEventArgs e)
		{
			if (Merging != null)
				Merging (this, e);
		}

		protected void OnMerged(MergedEventArgs e)
		{
			if (Merged != null)
				Merged (this, e);
		}

		protected void OnCommiting(CommitEventArgs e)
		{
			if (Commiting != null)
				Commiting (this, e);
		}

		protected void OnCommitted(CommitEventArgs e)
		{
			if (Committed != null)
				Committed (this, e);
		}

		protected void OnReverting(RevertEventArgs e)
		{
			if (Reverting != null)
				Reverting (this, e);
		}

		protected void OnReverted(RevertEventArgs e)
		{
			if (Reverted != null)
				Reverted (this, e);
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
					t.Commit ();
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
					t.Commit ();
				}
			}
		}

		internal byte[] CurrentBranchRaw
		{
			get {
				return (byte[])book.localConfig ["StellaVCS.Branch"];
			}
			set {
				using (var t = book.BeginTransaction ()) {
					book.localConfig ["StellaVCS.Branch"] = value;
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
			CheckNoLocalModifications ();
			CheckNoIncompleteMerge ();

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

			var e = new CheckoutEventArgs (CurrentRevisionRaw, goal.DbBranch.Revision);
			OnCheckouting (e);

			// First switch to the detached branch.
			CurrentBranchRaw = DetachedBranchId;
			CurrentRevisionRaw = originalBranchId;

			// Check out the target revision.
			SetCurrentRevisionImpl (goal.DbBranch.Revision);

			// Switch to the target branch.
			CurrentBranchRaw = goal.DbBranch.Name;

			OnCheckouted (e);
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
			CheckNoLocalModifications ();
			CheckNoIncompleteMerge ();

			// Ensure revision exists
			LookupRevision (revision);

			var e = new CheckoutEventArgs (CurrentRevisionRaw, revision);
			OnCheckouting (e);

			SetCurrentRevisionImpl (revision);

			OnCheckouted (e);
		}
		void SetCurrentRevisionImpl(byte[] revision)
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
			ret.CurrentToCommonAncestor = currentAncestors.Take (currentAncestors.Count - 1);

			// And then move the current revision to the goal revision.
			ret.CommonAncestorToGoal = goalAncestors.Take (goalAncestors.Count - 1).Reverse ();

			ret.CommonAncestor = currentAncestors [currentAncestors.Count - 1];

			return ret;
		}

		sealed class PathItem
		{
			public byte[] RevisionId;
			public Revision Revision;
			public PathItem Inflow; // Node to the input revision
			public int InputId;
		}

		Tuple<Revision[], Revision[]> 
		ComputePathToCommonAncestorOfRevisions(byte[] revisionId1, byte[] revisionId2)
		{
			ValidateRevisionId (revisionId1);
			ValidateRevisionId (revisionId2);

			// Perform a breadth first search
			var items = new Dictionary<byte[], PathItem> (StellaDB.DefaultKeyComparer.Instance);

			var rev1 = new PathItem () {
				RevisionId = revisionId1,
				InputId = 1,
				Revision = LookupRevision(revisionId1)
			};
			var rev2 = new PathItem () {
				RevisionId = revisionId2,
				InputId = 2,
				Revision = LookupRevision(revisionId2)
			};
			items.Add (rev1.RevisionId, rev1);
			items.Add (rev2.RevisionId, rev2);

			// (Latest revision is traversed first.)
			var queue = new Utils.PriorityQueue<double, PathItem>();
			queue.Enqueue (-rev1.Revision.DbRevision.Timestamp, rev1);
			queue.Enqueue (-rev2.Revision.DbRevision.Timestamp, rev2);

			PathItem common = null;
			PathItem anotherInflow = null;

			while (common == null && queue.Count > 0) {
				var item = queue.Dequeue ();
				var parents = item.Value.Revision.DbRevision.Parents;
				foreach (var parentId in parents) {
					PathItem pitem;
					if (items.TryGetValue(parentId, out pitem)) {
						if (item.Value.InputId == pitem.InputId) {
							// Meet up
							continue;
						} else {
							// Found the common ancestor.
							common = pitem;
							anotherInflow = item.Value;
							break;
						}
					} else {
						pitem = new PathItem () {
							RevisionId = parentId,
							InputId = item.Value.InputId,
							Revision = LookupRevision (parentId),
							Inflow = item.Value
						};
						items.Add (pitem.RevisionId, pitem);
						queue.Enqueue (-pitem.Revision.DbRevision.Timestamp, pitem);
					}
				}
			}

			if (common == null) {
				throw new InvalidOperationException (string.Format(
					"The common ancestor of revision {0} and {1} was not found.",
					RevisionIdToString(revisionId1),
					RevisionIdToString(revisionId2)));
			}

			var path = new List<Revision> ();

			for (var e = common; e != null; e = e.Inflow) {
				path.Add (e.Revision);
			}

			path.Reverse ();
			var path1 = path.ToArray();

			path.Clear ();
			common.Inflow = anotherInflow;
			for (var e = common; e != null; e = e.Inflow) {
				path.Add (e.Revision);
			}

			path.Reverse ();
			var path2 = path.ToArray();

			if (common.InputId == 1) {
				return Tuple.Create (path1, path2);
			} else {
				return Tuple.Create (path2, path1);
			}
		}

		/// <summary>
		/// Checks out the specified revision.
		/// </summary>
		/// <param name="revision">Revision.</param>
		public void CheckoutRevision(byte[] revision)
		{
			ValidateRevisionId (revision);

			CheckNoLocalModifications ();
			CheckNoIncompleteMerge ();

			var comparer = StellaDB.DefaultKeyComparer.Instance;
			if (comparer.Equals(revision, CurrentRevisionRaw)) {
				return;
			}

			// Switch to the detached branch
			var originalRevisionId = CurrentRevisionRaw;

			// Make sure revision exists
			LookupRevision (revision);
			var e = new CheckoutEventArgs (originalRevisionId, revision);
			OnCheckouting (e);

			CurrentBranchRaw = DetachedBranchId;
			CurrentRevisionRaw = originalRevisionId;

			SetCurrentRevisionImpl (revision);

			OnCheckouted (e);
		}

		#endregion

		#region Merge


		internal byte[] CurrentMergeTargetRevisionId
		{
			get {
				return (byte[])book.localConfig ["StellaVCS.MergeTargetRevision"];
			}
			set {
				using (var t = book.BeginTransaction ()) {
					book.localConfig ["StellaVCS.MergeTargetRevision"] = value;
					t.Commit ();
				}
			}
		}
		internal byte[] CurrentMergedRevisionId
		{
			get {
				return (byte[])book.localConfig ["StellaVCS.MergedRevision"];
			}
			set {
				using (var t = book.BeginTransaction ()) {
					book.localConfig ["StellaVCS.MergedRevision"] = value;
					t.Commit ();
				}
			}
		}
		internal byte[] CurrentMergeOriginId
		{
			get {
				return (byte[])book.localConfig ["StellaVCS.MergeOrigin"];
			}
			set {
				using (var t = book.BeginTransaction ()) {
					book.localConfig ["StellaVCS.MergeOrigin"] = value;
					t.Commit ();
				}
			}
		}

		public enum MergeResult
		{
			AlreadyUpToDate,
			MergedByFastForward,
			MergedByAutoMerge,
			MergeUnresolved
		}

		public MergeResult MergeRevision(byte[] mergedRevision)
		{
			ValidateRevisionId (mergedRevision);

			CheckNoLocalModifications ();
			CheckNoIncompleteMerge ();

			var currentRevision = CurrentRevisionRaw;
			var path = ComputePathToCommonAncestorOfRevisions (currentRevision, mergedRevision);
			var path1 = path.Item1;
			var path2 = path.Item2;
			var commonAncestor = path1.Last ();

			OnMerging (new MergingEventArgs (currentRevision, mergedRevision));

			if (commonAncestor == path2[0]) {
				OnMerged (new MergedEventArgs (currentRevision, currentRevision, mergedRevision, MergeResult.AlreadyUpToDate));
				return MergeResult.AlreadyUpToDate;
			} else if (commonAncestor == path1[0]) {
				// Fast forward.
				CurrentRevisionRaw = mergedRevision;

				OnMerged (new MergedEventArgs (currentRevision, mergedRevision, mergedRevision, MergeResult.MergedByFastForward));
				return MergeResult.MergedByFastForward;
			}

			// Non-fast-forward merge required.
			using (var t = book.BeginTransaction()) {
				CurrentMergeOriginId = commonAncestor.DbRevision.Id;
				CurrentMergeTargetRevisionId = currentRevision;
				CurrentMergedRevisionId = mergedRevision;

				SetCurrentRevisionImpl (commonAncestor.DbRevision.Id);

				// Check deltas
				{
					var cpath = ComputeCommitTreePath (commonAncestor.DbRevision.Id, currentRevision);
					if (cpath != null) {
						foreach (var r in cpath.CurrentToCommonAncestor) {
							RevertModificationsOfRevisionToMergeTable (r, false);
						}
						foreach (var r in cpath.CommonAncestorToGoal) {
							ApplyModificationsOfRevisionToMergeTable (r, false);
						}
					}
				}
				{
					var cpath = ComputeCommitTreePath (commonAncestor.DbRevision.Id, mergedRevision);
					if (cpath != null) {
						foreach (var r in cpath.CurrentToCommonAncestor) {
							RevertModificationsOfRevisionToMergeTable (r, true);
						}
						foreach (var r in cpath.CommonAncestorToGoal) {
							ApplyModificationsOfRevisionToMergeTable (r, true);
						}
					}
				}

				// Restore current revision
				SetCurrentRevisionImpl (currentRevision);

				// Try auto-merge
				bool automergeFailed = false;
				var succeededRowIds = new List<long> ();
				foreach (var r in mergeTable.Query(mergeTableEnumerateQuery)) {
					var item = r.ToObject<DbMergeItem> ();

					var merged = item.Value1 ?? item.Value2;

					if (item.Value1 != null && item.Value2 != null &&
						!StellaDB.DefaultKeyComparer.Instance.Equals(item.Value1, item.Value2)) {
						// Conflict!
						automergeFailed = true;

						// Try to choose the latest one
						if (item.Time1 > item.Time2)
							merged = item.Value1;
						else
							merged = item.Value2;
					}

					var table = GetTable (item.Table);
					table.UpdateRaw (item.RowId, merged);
					succeededRowIds.Add (r.RowId);
				}

				// Delete all succeeded merge
				foreach (var r in succeededRowIds) {
					mergeTable.Delete (r);
				}

				if (!automergeFailed) {
					// Automerge succeeded.

					CommitLocalModifications (GenerateMergeMessage (currentRevision, mergedRevision));
					t.Commit ();

					OnMerged (new MergedEventArgs (currentRevision, CurrentRevisionRaw, mergedRevision, MergeResult.MergedByAutoMerge));
					return MergeResult.MergedByAutoMerge;
				}

				// Automerge failed.

				t.Commit ();
				OnMerged (new MergedEventArgs (currentRevision, null, mergedRevision, MergeResult.MergeUnresolved));
				return MergeResult.MergeUnresolved;
			}
		}

		static string GenerateMergeMessage(byte[] target, byte[] fromRevision)
		{
			return string.Format ("Merged {0} to {1}",
				RevisionIdToString(fromRevision),
				RevisionIdToString(target));
		}

		public bool HasUnresolvedMerge
		{
			get {
				return mergeTable.Query(mergeTableEnumerateQuery).Any();
			}
		}

		void CheckNoIncompleteMerge()
		{
			if (HasUnresolvedMerge) {
				throw new InvalidOperationException ("There's an ongoing unresolved merge.");
			}
		}

		internal struct ValueAndRevision
		{
			public byte[] Value;
			public DateTime Timestamp;
			public byte[] Revision;

			public ValueAndRevision(byte[] revision, DateTime timestamp, byte[] value)
			{
				Revision = revision;
				Timestamp = timestamp;
				Value = value;
			}
		}

		internal sealed class ConflicingRow
		{
			public VersionControlledTable Table;
			public long RowId;
			public ValueAndRevision Original;
			public ValueAndRevision Current;
			public ValueAndRevision Merged;
		}

		IEnumerable<ConflicingRow> GetConflictingRowsImpl(IEnumerable<DbMergeItem> items)
		{
			var originalRev = LookupRevision (CurrentMergeOriginId);
			return from item in items
				select new ConflicingRow () 
			{
				Table = GetTable(item.Table),
				RowId = item.RowId,
				Original = new ValueAndRevision(
					originalRev.DbRevision.Id,
					DateTime.FromOADate(originalRev.DbRevision.Timestamp),
					item.Original
				),
				Current = new ValueAndRevision(
					item.Revision1,
					DateTime.FromOADate(item.Time1),
					item.Value1
				),
				Merged = new ValueAndRevision(
					item.Revision2,
					DateTime.FromOADate(item.Time2),
					item.Value2
				)
			};
		}

		internal IEnumerable<ConflicingRow> GetConflictingRows()
		{
			return GetConflictingRowsImpl (from r in mergeTable.Query(mergeTableEnumerateQuery)
				select r.ToObject<DbMergeItem>());
		}

		internal IEnumerable<ConflicingRow> GetConflictingRows(string tableName)
		{
			return GetConflictingRowsImpl (from r in mergeTable.Query(mergeTableFindByTableQuery(utf8.GetBytes(tableName)))
				select r.ToObject<DbMergeItem>());
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
				foreach (var deltaRow in deltaTable.Query(deltaTableFindByRevisionQuery(r.DbRevision.Id))) {
					DbDelta delta = deltaRow.ToObject<DbDelta> ();
					VersionControlledTableImpl vctable = GetTableImpl (delta.Table);
					StellaDB.Table table = vctable.BaseTable;
					byte[] tableRow = table.FetchRaw (delta.RowId);
					byte[] original = tableRow ?? new byte[] { };
					byte[] updated = deltaEncoder.DecodeY (delta.Delta, original);
					vctable.RowBeingUpdatedByVersionControl (delta.RowId, original, updated);
					if (updated.Length == 0) {
						table.Delete (delta.RowId);
					} else {
						table.UpdateRaw (delta.RowId, updated);
					}
				}

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
				foreach (var deltaRow in deltaTable.Query(deltaTableFindByRevisionQuery(r.DbRevision.Id))) {
					DbDelta delta = deltaRow.ToObject<DbDelta> ();
					VersionControlledTableImpl vctable = GetTableImpl (delta.Table);
					StellaDB.Table table = vctable.BaseTable;
					byte[] tableRow = table.FetchRaw (delta.RowId);
					byte[] original = tableRow ?? new byte[] { };
					byte[] updated = deltaEncoder.DecodeX (delta.Delta, original);
					vctable.RowBeingUpdatedByVersionControl (delta.RowId, original, updated);
					if (updated.Length == 0) {
						table.Delete (delta.RowId);
					} else {
						table.UpdateRaw (delta.RowId, updated);
					}
				}

				CurrentRevisionRaw = r.DbRevision.ReferenceRevision;

				t.Commit ();
			}
		}

		void ApplyModificationsOfRevisionToMergeTable(Revision r, bool updateValue2)
		{
			using (var t = book.BeginTransaction()) {
				foreach (var deltaRow in deltaTable.Query(deltaTableFindByRevisionQuery(r.DbRevision.Id))) {
					var delta = deltaRow.ToObject<DbDelta> ();
					var itemRow = mergeTable.Query (mergeTableLookupQuery (delta.Table, delta.RowId)).FirstOrDefault ();
					bool created = false;
					DbMergeItem item;
					if (itemRow == null) {
						created = true;
						item = new DbMergeItem () {
							Table = delta.Table,
							RowId = delta.RowId
						};

						var table = GetTableImpl (delta.Table).BaseTable;
						var tableRow = table.FetchRaw (delta.RowId);
						item.Original = tableRow ?? new byte[] { };
					} else {
						item = itemRow.ToObject<DbMergeItem> ();
					}

					var updated = deltaEncoder.DecodeY (delta.Delta, 
						(updateValue2 ? item.Value2 : item.Value1) ?? item.Original);
					if (updateValue2) {
						item.Value2 = updated;
						item.Revision2 = r.DbRevision.Id;
						item.Time2 = r.DbRevision.Timestamp;
					} else {
						item.Value1 = updated;
						item.Revision1 = r.DbRevision.Id;
						item.Time1 = r.DbRevision.Timestamp;
					}

					if (created) {
						mergeTable.Insert (item, false);
					} else {
						mergeTable.Update (itemRow.RowId, item);
					}
				}

				t.Commit ();
			}
		}
		void RevertModificationsOfRevisionToMergeTable(Revision r, bool updateValue2)
		{
			using (var t = book.BeginTransaction()) {
				var prev = LookupRevision (r.DbRevision.ReferenceRevision);
				foreach (var deltaRow in deltaTable.Query(deltaTableFindByRevisionQuery(r.DbRevision.Id))) {
					var delta = deltaRow.ToObject<DbDelta> ();
					var itemRow = mergeTable.Query (mergeTableLookupQuery (delta.Table, delta.RowId)).FirstOrDefault ();
					bool created = false;
					DbMergeItem item;
					if (itemRow == null) {
						created = true;
						item = new DbMergeItem () {
							Table = delta.Table,
							RowId = delta.RowId
						};

						var table = GetTableImpl (delta.Table);
						var tableRow = table.FetchRaw (delta.RowId);
						item.Original = tableRow ?? new byte[] { };
					} else {
						item = itemRow.ToObject<DbMergeItem> ();
					}

					var updated = deltaEncoder.DecodeX (delta.Delta, 
						(updateValue2 ? item.Value2 : item.Value1) ?? item.Original);
					if (updateValue2) {
						item.Value2 = updated;
						item.Revision2 = prev.DbRevision.Id;
						item.Time2 = prev.DbRevision.Timestamp;
					} else {
						item.Value1 = updated;
						item.Revision1 = prev.DbRevision.Id;
						item.Time1 = prev.DbRevision.Timestamp;
					}

					if (created) {
						mergeTable.Insert (item, false);
					} else {
						mergeTable.Update (itemRow.RowId, item);
					}
				}

				t.Commit ();
			}
		}

		#endregion

		#region Local Modifications

		static readonly byte[] LocalModificationRevisionId = new byte[] {};

		// Not thread-safe. Don't make it static.
		readonly BitDelta.DeltaEncoder deltaEncoder = new BitDelta.DeltaEncoder();

		readonly Dictionary<string, VersionControlledTableImpl> tables =
			new Dictionary<string, VersionControlledTableImpl> ();
		readonly Dictionary<byte[], VersionControlledTableImpl> tablesByBytes =
			new Dictionary<byte[], VersionControlledTableImpl> (StellaDB.DefaultKeyComparer.Instance);

		sealed class VersionControlledTableImpl: VersionControlledTable
		{
			readonly VersionController vc;
			readonly byte[] tableNameBytes;

			public VersionControlledTableImpl(VersionController vc, string tableName, StellaDB.Table table):
			base(vc.book, table, tableName)
			{
				this.vc = vc;
				tableNameBytes = utf8.GetBytes(tableName);
			}

			public void RowBeingUpdatedByVersionControl(long rowId, byte[] oldData, byte[] newData)
			{
				OnUpdate (new VersionControlledTableUpdatedEventArgs (rowId, oldData, newData,
					VersionControlledTableUpdateReason.VersionController));
			}

			protected override void RowBeingUpdated (long rowId, byte[] oldData, byte[] newData)
			{
				foreach (var deltaRow in vc.deltaTable.Query(vc.deltaTableLookupQuery(LocalModificationRevisionId, tableNameBytes, rowId))) {
					// There's a delta
					var delta = deltaRow.ToObject<DbDelta> ();
					byte[] original = vc.deltaEncoder.DecodeX (delta.Delta, oldData);
					byte[] newDelta = vc.deltaEncoder.Encode (original, newData);
					if (newDelta.Length == 0) {
						// Same as the original.
						vc.deltaTable.Delete (deltaRow.RowId);
					} else {
						delta.Delta = newDelta;
						vc.deltaTable.Update (deltaRow.RowId, delta);
					}
					return;
				}

				// Delta not found (local modification is not yet made)
				{
					var delta = new DbDelta ();
					delta.Revision = LocalModificationRevisionId;
					delta.Table = tableNameBytes;
					delta.RowId = rowId;
					delta.Delta = vc.deltaEncoder.Encode (oldData, newData);
					if (delta.Delta.Length == 0) {
						return;
					}
					vc.deltaTable.Insert (delta, false);
				}
			}

		}

		public void CommitLocalModifications(string message)
		{
			if (!HasLocalModifications()) {
				throw new InvalidOperationException ("There are no local modifications.");
			}
			var revisionId = GenerateRevisionId ();
			var e = new CommitEventArgs (CurrentRevisionRaw, revisionId);
			OnCommiting (e);

			using (var t = book.BeginTransaction()) {

				var rev = new DbRevision () {
					Id = revisionId,
					Message = message,
					DateTime = DateTime.UtcNow,
					Parents = new [] {
						CurrentRevisionRaw
					}
				};

				var rowIds = new List<long> ();
				foreach (var deltaRow in deltaTable.Query (deltaTableFindByRevisionQuery (LocalModificationRevisionId))) {
					rowIds.Add (deltaRow.RowId);
				}
				foreach (var rowId in rowIds) {
					var delta = deltaTable.Fetch (rowId).ToObject<DbDelta> ();
					delta.Revision = revisionId;
					deltaTable.Update (rowId, delta);
				}

				revisionTable.Insert (rev, false);

				if (HasUnresolvedMerge) {
					// Unresolved merge was resolved.
					var rowIds2 =
						from r in mergeTable.Query(mergeTableEnumerateQuery)
						select r.RowId;
					foreach (var r in rowIds2.ToArray()) {
						mergeTable.Delete(r);
					}
				}

				CurrentRevisionRaw = revisionId;

				t.Commit ();
			}

			OnCommitted (e);
		}

		public void RevertLocalModifications()
		{
			OnReverting (new RevertEventArgs ());
			using (var t = book.BeginTransaction()) {
				var rowIds = new List<long> ();
				foreach (var deltaRow in deltaTable.Query (deltaTableFindByRevisionQuery (LocalModificationRevisionId))) {
					DbDelta delta = deltaRow.ToObject<DbDelta> ();
					VersionControlledTableImpl vctable = GetTableImpl (delta.Table);
					StellaDB.Table table = vctable.BaseTable;
					byte[] tableRow = table.FetchRaw (delta.RowId);
					byte[] original = tableRow ?? new byte[] { };
					byte[] updated = deltaEncoder.DecodeX (delta.Delta, original);
					vctable.RowBeingUpdatedByVersionControl (delta.RowId, original, updated);
					if (updated.Length == 0) {
						table.Delete (delta.RowId);
					} else {
						table.UpdateRaw (delta.RowId, updated);
					}
					rowIds.Add (deltaRow.RowId);
				}
				foreach (var row in rowIds)
					deltaTable.Delete (row);

				// Cancel merge
				rowIds.Clear ();
				foreach (var r in mergeTable.Query(mergeTableEnumerateQuery)) {
					rowIds.Add (r.RowId);
				}
				foreach (var row in rowIds)
					mergeTable.Delete (row);
				t.Commit ();
			}
			OnReverted (new RevertEventArgs ());
		}

		public bool HasLocalModifications()
		{
			return deltaTable.Query (deltaTableFindByRevisionQuery (LocalModificationRevisionId)).Any ();
		}

		public void CheckNoLocalModifications()
		{
			if (HasLocalModifications()) {
				throw new InvalidOperationException ("Cannot perform this operation when there are any local modifications.");
			}
		}

		VersionControlledTableImpl GetTableImpl(string name)
		{
			VersionControlledTableImpl table;
			if (tables.TryGetValue(name, out table)) {
				return table;
			}
			table = new VersionControlledTableImpl (this, name, book.database [name]);
			tables.Add (name, table);
			tablesByBytes.Add (utf8.GetBytes (name), table);
			return table;
		}

		VersionControlledTableImpl GetTableImpl(byte[] bytes)
		{
			VersionControlledTableImpl table;
			if (tablesByBytes.TryGetValue(bytes, out table)) {
				return table;
			}
			string name = utf8.GetString (bytes);
			table = new VersionControlledTableImpl (this, name, book.database [name]);
			tables.Add (name, table);
			tablesByBytes.Add (bytes, table);
			return table;
		}

		public VersionControlledTable GetTable(string name)
		{
			return GetTableImpl(name);
		}
		internal VersionControlledTable GetTable(byte[] name)
		{
			return GetTableImpl(name);
		}

		#endregion
	}

	public enum VersionControlledTableUpdateReason
	{
		VersionController,
		TableUpdate
	}

	public sealed class VersionControlledTableUpdatedEventArgs: EventArgs
	{
		internal byte[] oldValue, newValue;
		readonly long rowId;
		readonly VersionControlledTableUpdateReason reason;

		internal VersionControlledTableUpdatedEventArgs(long rowId, byte[] oldValue, byte[] newValue,
			VersionControlledTableUpdateReason reason)
		{
			this.rowId = rowId;
			this.oldValue = oldValue;
			this.newValue = newValue;
			this.reason = reason;
		}

		public long RowId
		{
			get { return rowId; }
		}

		public byte[] GetOldValue ()
		{
			return (byte[])oldValue.Clone();
		}

		public byte[] GetNewValue()
		{
			return (byte[])newValue.Clone ();
		}

		public VersionControlledTableUpdateReason Reason
		{ 
			get { return reason; }
		}
	}

	public abstract class VersionControlledTable
	{
		readonly LogBook book;
		readonly StellaDB.Table baseTable;
		readonly string name;

		public event EventHandler<VersionControlledTableUpdatedEventArgs> Updated;

		protected VersionControlledTable(LogBook book, StellaDB.Table baseTable, string name)
		{
			this.book = book;
			this.baseTable = baseTable;
			this.name = name;
		}

		protected void OnUpdate(VersionControlledTableUpdatedEventArgs e)
		{
			if (Updated != null)
				Updated (this, e);
		}

		public LogBook LogBook
		{
			get { return book;}
		}

		public string Name
		{
			get { return name; }
		}

		public StellaDB.Table BaseTable
		{
			get { return baseTable; }
		}

		protected abstract void RowBeingUpdated (long rowId, byte[] oldData, byte[] newData);

		void RowBeingUpdatedWrapper (long rowId, byte[] oldData, byte[] newData)
		{
			OnUpdate (new VersionControlledTableUpdatedEventArgs (rowId,
				oldData, newData, VersionControlledTableUpdateReason.TableUpdate));
			RowBeingUpdated (rowId, oldData, newData);
		}



		public void UpdateRaw (long rowId, byte[] data)
		{
			if (data.Length == 0) {
				Delete (rowId);
				return;
			}

			var cmp = StellaDB.DefaultKeyComparer.Instance;
			var oldData = FetchRaw (rowId);
			if (oldData == null) {
				RowBeingUpdatedWrapper (rowId, new byte[] {}, data);
				baseTable.InsertRaw (rowId, data, false);
			} else {
				if (cmp.Equals (oldData, data)) {
					return;
				}
				RowBeingUpdatedWrapper (rowId, oldData, data);
				baseTable.UpdateRaw (rowId, data);
			}
		}
		public void Update(long rowId, object data)
		{
			UpdateRaw (rowId, baseTable.Serializer.Serialize (data));
		}

		public void Delete(long rowId)
		{
			var oldData = FetchRaw (rowId);
			if (oldData == null) {
				return;
			}
			RowBeingUpdatedWrapper (rowId, oldData, new byte[] {});
			baseTable.Delete (rowId);
		}

		public long InsertRaw(byte[] data)
		{
			var rowId = baseTable.InsertRaw (data, false);
			try {
				RowBeingUpdatedWrapper (rowId, new byte[] { }, data);
			} catch {
				baseTable.Delete (rowId);
				throw;
			}
			return rowId;
		}

		public long Insert(object obj)
		{
			return InsertRaw (baseTable.Serializer.Serialize (obj));
		}


		public byte[] FetchRaw(long rowId)
		{
			return baseTable.FetchRaw(rowId);
		}
		public StellaDB.Table.ResultRow Fetch(long rowId)
		{
			return baseTable.Fetch(rowId);
		}

		public StellaDB.PreparedQuery Prepare(
			System.Linq.Expressions.Expression<Func<long, StellaDB.Ston.StonVariant, bool>> predicate)
		{
			return baseTable.Prepare (predicate);
		}

		public IEnumerable<StellaDB.Table.ResultRow> Query (StellaDB.PreparedQuery stmt)
		{
			return baseTable.Query (stmt);
		}


	}

	public sealed class VersionControlledTableBufferedEventProxy
	{
		public event EventHandler<VersionControlledTableUpdatedEventArgs> Updated;

		readonly VersionControlledTable table;

		sealed class Update
		{
			public byte[] Before, After;
		}
		Dictionary<long, Update> deferredUpdates = 
			new Dictionary<long, Update> ();
		bool defer = false;

		public VersionControlledTableBufferedEventProxy(VersionControlledTable table)
		{
			this.table = table;
			table.Updated += HandleUpdated;
			table.LogBook.VersionController.Checkouting += HandleCheckouting;
			table.LogBook.VersionController.Checkouted += HandleCheckouted;
			table.LogBook.VersionController.Merging += HandleMerging;
			table.LogBook.VersionController.Merged += HandleMerged;
			table.LogBook.VersionController.Reverting += HandleReverting;
			table.LogBook.VersionController.Reverted += HandleReverted;
		}

		public VersionControlledTable Table
		{
			get { return table; }
		}

		void OnUpdate(VersionControlledTableUpdatedEventArgs e)
		{
			if (Updated != null)
				Updated (this, e);
		}

		void StartUpdate()
		{
			defer = true;
		}

		void EndUpdate()
		{
			defer = false;

			var upd = deferredUpdates;
			if (upd.Count == 0) {
				return;
			}
			deferredUpdates = new Dictionary<long, Update> ();
			foreach (var u in upd) {
				if (StellaDB.DefaultKeyComparer.Instance.Equals (u.Value.Before, u.Value.After))
					continue;

				OnUpdate (new VersionControlledTableUpdatedEventArgs(u.Key, u.Value.Before, u.Value.After,
					VersionControlledTableUpdateReason.VersionController));
			}
		}

		void HandleReverted (object sender, RevertEventArgs e)
		{
			EndUpdate ();
		}

		void HandleReverting (object sender, RevertEventArgs e)
		{
			StartUpdate ();
		}

		void HandleMerged (object sender, MergedEventArgs e)
		{
			EndUpdate ();
		}

		void HandleMerging (object sender, MergingEventArgs e)
		{
			StartUpdate ();
		}

		void HandleCheckouted (object sender, CheckoutEventArgs e)
		{
			EndUpdate ();
		}

		void HandleCheckouting (object sender, CheckoutEventArgs e)
		{
			StartUpdate ();
		}

		public void RemoveEventHandlers()
		{
			table.Updated -= HandleUpdated;
			table.LogBook.VersionController.Checkouting -= HandleCheckouting;
			table.LogBook.VersionController.Checkouted -= HandleCheckouted;
			table.LogBook.VersionController.Merging -= HandleMerging;
			table.LogBook.VersionController.Merged -= HandleMerged;
			table.LogBook.VersionController.Reverting -= HandleReverting;
			table.LogBook.VersionController.Reverted -= HandleReverted;
		}

		void HandleUpdated (object sender, VersionControlledTableUpdatedEventArgs e)
		{
			if (defer) {
				Update update;
				if (deferredUpdates.TryGetValue(e.RowId, out update)) {
					update.After = e.newValue;
				} else {
					update = new Update () {
						Before = e.oldValue,
						After = e.newValue
					};
					deferredUpdates.Add (e.RowId, update);
				}
			} else {
				OnUpdate (e);
			}
		}
	}
}


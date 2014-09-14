using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Yavit.StellaDB
{
	public abstract class PreparedQuery: MarshalByRefObject
	{
		readonly internal Table table;
		internal int TableStructureState;
		internal Func<Indexer.QueryOptimizer.ProcessResult> PlanBuilder;

		internal PreparedQuery(Table t)
		{
			this.table = t;
		}

		internal abstract Expression<Func<long, Ston.StonVariant, bool>> Predicate
		{ get; }
	}

	public partial class Table
	{
		int tableStructureState = 0;

		#region Raw Row Access
		void InternalInsertRaw(long rowId, byte[] value, bool updateOnDuplicate, bool errorOnNotFound)
		{
			EnsureStoreCreated ();

			EncodeRowId (rowIdBuffer, rowId);
			LowLevel.IKeyValueStoreEntry entry;

			if ((entry = store.FindEntry (rowIdBuffer)) != null) {
				if (updateOnDuplicate) {
					try {
						var reader = new Ston.StonReader (entry.ReadValue());
						var val = new Ston.SerializedStonVariant (reader);
						PrepareIndexBeforeUpdatingRow(rowId, val);
					}
					catch (Ston.StonException) { }
					catch (Ston.StonVariantException) { }

					entry.WriteValue (value);

					try {
						var reader = new Ston.StonReader (value);
						var val = new Ston.SerializedStonVariant (reader);
						UpdateIndexAfterUpdatingRow(rowId, val);
					}
					catch (Ston.StonException) { }
					catch (Ston.StonVariantException) { }
					return;
				} else {
					throw new InvalidOperationException ("Row with the specified row ID already exists.");
				}
			}

			if (errorOnNotFound) {
				throw new InvalidOperationException ("Specified row ID was not found.");
			}

			store.InsertEntry (rowIdBuffer).WriteValue (value);

			try {
				var reader = new Ston.StonReader (value);
				var val = new Ston.SerializedStonVariant (reader);
				InsertRowToIndex(rowId, val);
			}
			catch (Ston.StonException) { }
			catch (Ston.StonVariantException) { }
		}

		public void InsertRaw(long rowId, byte[] value, bool updateOnDuplicate)
		{
			InternalInsertRaw (rowId, value, updateOnDuplicate, false);
			database.DoAutoCommit ();
		}

		public long InsertRaw(byte[] value, bool updateOnDuplicate)
		{
			var rowId = AutoIncrementRowIdValue;
			InsertRaw (AutoIncrementRowIdValue, value, updateOnDuplicate);
			++AutoIncrementRowIdValue;
			database.DoAutoCommit ();
			return rowId;
		}

		public void UpdateRaw(long rowId, byte[] value)
		{
			InternalInsertRaw (rowId, value, true, true);
			database.DoAutoCommit ();
		}

		public byte[] FetchRaw(long rowId)
		{
			EnsureStoreCreated ();

			EncodeRowId (rowIdBuffer, rowId);
			LowLevel.IKeyValueStoreEntry entry;

			if ((entry = store.FindEntry (rowIdBuffer)) != null) {
				return entry.ReadValue ();
			}

			return null;
		}

		public void Delete(long rowId)
		{
			EnsureStoreCreated ();

			EncodeRowId (rowIdBuffer, rowId);

			var r = store.FindEntry (rowIdBuffer);
			if (r != null) {
				try {
					var reader = new Ston.StonReader (r.ReadValue());
					var val = new Ston.SerializedStonVariant (reader);
					DeleteRowFromIndex(rowId, val);
				}
				catch (Ston.StonException) { }
				catch (Ston.StonVariantException) { }

				store.DeleteEntry (rowIdBuffer);

				database.DoAutoCommit ();
			}
		}
		#endregion

		#region Row Access

		[Serializable]
		public class ResultRow
		{
			readonly Table table;
			readonly LowLevel.IKeyValueStoreEntry entry;
			long rowId;
			byte[] data;
			Ston.StonVariant variant;

			internal ResultRow(Table table, long rowId, byte[] data)
			{
				this.table = table;
				this.entry = null;
				this.rowId = rowId;
				this.data = data;
			}

			void EnsureDataLoaded()
			{
				if (data == null) {
					data = entry.ReadValue ();
				}
			}

			public byte[] GetBuffer()
			{
				EnsureDataLoaded ();
				return data;
			}

			public Ston.StonReader CreateReader()
			{
				return new Ston.StonReader (GetBuffer ());
			}

			public Ston.StonVariant ToVariant()
			{
				if (object.ReferenceEquals(variant, null)) {
					variant = new Ston.SerializedStonVariant (CreateReader ());
				}
				return variant;
			}

			public T ToObject<T>()
			{
				table.CheckHasSerializer ();
				EnsureDataLoaded ();
				return table.serializer.Deserialize<T> (data);
			}
			public object ToObject(Type type)
			{
				table.CheckHasSerializer ();
				EnsureDataLoaded ();
				return table.serializer.Deserialize (data, type);
			}

			public object ToObject()
			{
				table.CheckHasSerializer ();
				EnsureDataLoaded ();
				return table.serializer.DeserializeObject (data);
			}

			public long RowId
			{
				get { return rowId; }
			}
		}

		Ston.StonSerializer serializer = new Ston.StonSerializer();

		public Ston.StonSerializer Serializer {
			get {
				return serializer;
			}
			set {
				serializer = value;
			}
		}
		void CheckHasSerializer()
		{
			if (serializer == null) {
				throw new InvalidOperationException("Serializer is null.");
			}
		}

		public void Insert(long rowId, object obj, bool updateOnDuplicate)
		{
			CheckHasSerializer ();
			InsertRaw (rowId, serializer.Serialize (obj), updateOnDuplicate);
		}
		public long Insert(object obj, bool updateOnDuplicate)
		{
			CheckHasSerializer ();
			return InsertRaw (serializer.Serialize (obj), updateOnDuplicate);
		}
		public void Update(long rowId, object obj)
		{
			CheckHasSerializer ();
			UpdateRaw (rowId, serializer.Serialize (obj));
		}

		public ResultRow Fetch(long rowId)
		{
			var data = FetchRaw (rowId);
			if (data == null) {
				return null;
			} else {
				return new ResultRow (this, rowId, data);
			}
		}

		sealed class BasicPreparedQuery: PreparedQuery
		{
			readonly Expression<Func<long, Ston.StonVariant, bool>> predicate;

			public BasicPreparedQuery(Table table, Expression<Func<long, Ston.StonVariant, bool>> predicate):
			base(table)
			{
				this.predicate = predicate;
			}
			internal override Expression<Func<long, Yavit.StellaDB.Ston.StonVariant, bool>> Predicate {
				get {
					return predicate;
				}
			}
		}
		// TODO: sort
		public PreparedQuery Prepare(Expression<Func<long, Ston.StonVariant, bool>> predicate)
		{
			EnsureQueryOptimizer ();
			return new BasicPreparedQuery (this, predicate);
		}

		public PreparedTextQuery Prepare(string expr)
		{
			EnsureQueryOptimizer ();
			return new PreparedTextQuery (this, expr);
		}

		public IEnumerable<ResultRow> Query(PreparedQuery stmt)
		{
			if (stmt == null)
				throw new ArgumentNullException ("stmt");

			if (stmt.table != this)
				throw new ArgumentException ("PreparedQuery cannot be used with the other table than onw which created PreparedQuery.", "stmt");

			if (stmt.PlanBuilder == null ||
				tableStructureState != stmt.TableStructureState) {
				EnsureQueryOptimizer ();
				var planBuilder = optimizer.Process (stmt.Predicate, new Indexer.QueryOptimizer.SortKey[0]);
				stmt.PlanBuilder = planBuilder;
				stmt.TableStructureState = tableStructureState;
			}
			var plan = stmt.PlanBuilder ();

			IEnumerable<ResultRow> unsorted;
			if (store != null && plan.RowIdUsage != null) {
				unsorted = QueryByTableScan (tableStructureState, plan.RowIdUsage);
			} else if (store != null && plan.IndexUsage != null) {
				unsorted = QueryByIndex (tableStructureState, plan.IndexUsage);
			} else {
				unsorted = Enumerable.Empty<ResultRow> ();
			}

			unsorted = unsorted.Where (row => { 
				try {
					return plan.Expression (row.RowId, row.ToVariant ());
				} catch (Ston.StonVariantException) {
					return false;
				}
			});

			// TODO: sort
			return unsorted;
		}

		void CheckMatchStructureState(int state)
		{
			if (tableStructureState != state || store == null) {
				throw new InvalidOperationException("Table structure was modified or a transaction was rollbacked.");
			}
		}

		IEnumerable<ResultRow> QueryByTableScan(int strState, Indexer.QueryOptimizer.RowIdUsage plan)
		{
			CheckMatchStructureState (strState);
			EncodeRowId (rowIdBuffer, plan.StartRowId ?? (plan.Descending ? long.MaxValue : 0));
			IEnumerable<LowLevel.IKeyValueStoreEntry> entries = 
				plan.Descending ? store.EnumerateEntiresInDescendingOrder (rowIdBuffer) :
				store.EnumerateEntiresInAscendingOrder (rowIdBuffer);
			foreach (var entry in entries) {
				long rowId = DecodeRowIdForKey (entry.GetKey ());
				if (plan.EndRowId != null &&
					(plan.Descending ? rowId < (long)plan.EndRowId :
						rowId > (long)plan.EndRowId)) {
					break;
				}

				var data = entry.ReadValue ();
				yield return new ResultRow (this, rowId, data);
				CheckMatchStructureState (strState);
			}
		}
		IEnumerable<ResultRow> QueryByIndex(int strState, Indexer.QueryOptimizer.IndexUsage plan)
		{
			CheckMatchStructureState (strState);
			var index = (QOIndex) plan.Index;
			var tableIndex = index.TableIndex;
			var istore = tableIndex.Store;
			var entries = plan.Descending ? istore.EnumerateEntiresInDescendingOrder (plan.StartKey) :
				istore.EnumerateEntiresInAscendingOrder (plan.StartKey);
			var comparer = tableIndex.Index.KeyComparer;
			foreach (var entry in entries) {
				var key = entry.GetKey ();
				if (plan.EndKey != null) {
					if (plan.EndInclusive) {
						if (plan.Descending ?
							comparer.Compare(key, 0, key.Length, plan.EndKey, 0, plan.EndKey.Length) <= 0 :
							comparer.Compare(key, 0, key.Length, plan.EndKey, 0, plan.EndKey.Length) >= 0) {
							break;
						}
					} else {
						if (plan.Descending ?
							comparer.Compare(key, 0, key.Length, plan.EndKey, 0, plan.EndKey.Length) < 0 :
							comparer.Compare(key, 0, key.Length, plan.EndKey, 0, plan.EndKey.Length) > 0) {
							break;
						}
					}
				}

				// Indirect access
				long rowId = tableIndex.Index.GetRowId (key, 0);
				EncodeRowId (rowIdBuffer, rowId);
				var e = store.FindEntry (rowIdBuffer);
				if (e != null) {
					var data = e.ReadValue ();
					yield return new ResultRow (this, rowId, data);
					CheckMatchStructureState (strState);
				}
			}
		}

		#endregion

	}
}


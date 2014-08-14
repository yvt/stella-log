using System;

namespace Yavit.StellaDB
{
	public class Database
	{
		Transaction currentTransaction = null;
		LowLevel.LowLevelDatabase lldb;

		public Database ()
		{
			throw new NotImplementedException ();
		}

		void OpenDatabase()
		{
			throw new NotImplementedException ();
		}

		internal void DoAutoCommit()
		{
			if (currentTransaction == null) {
				Commit ();
			}
		}

		internal void Commit()
		{
			lldb.Flush ();
			currentTransaction = null;
		}
		internal void Rollback()
		{
			// Reopnes database to do rollback.
			// This assumes WAL journaling is being used.
			OpenDatabase ();

			// TODO: reset other objects
			currentTransaction = null;
		}

		public Transaction BeginTransaction()
		{
			if (currentTransaction != null) {
				currentTransaction.Rollback ();
			}
			currentTransaction = new Transaction (this);
			return currentTransaction;
		}
	}
}


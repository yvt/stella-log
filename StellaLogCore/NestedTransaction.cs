using System;
using System.Collections.Generic;

namespace Yavit.StellaLog.Core
{
	sealed class NestedTransactionManager: IDisposable
	{
		readonly StellaDB.Database database;

		StellaDB.ITransaction currentDbTransaction = null;
		readonly Stack<NestedTransaction> stack = new Stack<NestedTransaction>();

		public NestedTransactionManager(StellaDB.Database database)
		{
			this.database = database;
		}

		public void Dispose ()
		{
			while (stack.Count > 0) {
				stack.Pop ().Rollbacked ();
			}
			if (currentDbTransaction != null) {
				currentDbTransaction.Dispose ();
			}
		}

		public INestedTransaction BeginTransaction()
		{
			var t = new NestedTransaction (this);
			stack.Push (t);
			if (stack.Count == 1) {
				currentDbTransaction = database.BeginTransaction ();
			}
			return t;
		}

		void Rollback(NestedTransaction t)
		{
			if (stack.Count == 0 || stack.Peek() != t) {
				throw new InvalidOperationException ("Outer transaction must be rolled back before " +
				"disposing the inner transaction.");
			}

			// All current transactions must be rolled back
			currentDbTransaction.Dispose ();
			currentDbTransaction = null;
			while (stack.Count > 0) {
				stack.Pop ().Rollbacked ();
			}
		}

		void Commit(NestedTransaction t)
		{
			if (stack.Count == 0 || stack.Peek() != t) {
				throw new InvalidOperationException ("Outer transaction must be rolled back or commited before " +
					"commiting the inner transaction.");
			}

			stack.Pop ();

			if (stack.Count == 0) {
				currentDbTransaction.Commit ();
				currentDbTransaction.Dispose ();
				currentDbTransaction = null;
			}
		}

		enum State
		{
			NotCommited,
			Commited,
			Rollbacked
		}

		sealed class NestedTransaction: INestedTransaction, IDisposable
		{
			readonly NestedTransactionManager manager;
			State state = State.NotCommited;

			public NestedTransaction (NestedTransactionManager manager)
			{
				this.manager = manager;
			}

			public void Rollbacked()
			{
				state = State.Rollbacked;
			}

			public void Commit ()
			{
				if (state == State.Commited) {
					throw new InvalidOperationException ("Transaction is already commited.");
				}
				if (state == State.Rollbacked) {
					throw new InvalidOperationException ("Transaction is already rollbacked. " +
						"This might be because an inner transaction might have been rollbacked.");
				}
				manager.Commit (this);
				state = State.Commited;
			}

			public void Dispose ()
			{
				if (state == State.NotCommited) {
					manager.Rollback (this);
					if (state != State.Rollbacked) {
						throw new InvalidOperationException ();
					}
				}
			}
		}
	}
	public interface INestedTransaction: IDisposable
	{
		void Commit();
	}
}


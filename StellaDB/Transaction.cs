using System;

namespace Yavit.StellaDB
{
	public class Transaction: IDisposable
	{
		Database database;
		bool commited = false;
		bool rollbacked = false;

		internal Transaction (Database database)
		{
			this.database = database;
		}

		void CheckState()
		{
			if (commited) {
				throw new InvalidOperationException ("Transaction is already commited.");
			}
			if (rollbacked) {
				throw new InvalidOperationException ("Transaction is already rollbacked.");
			}
		}

		public void Commit ()
		{
			CheckState ();
			database.Commit ();
			commited = true;
		}

		public void Rollback ()
		{
			CheckState ();
			database.Rollback ();
			rollbacked = true;
		}

		public void Dispose ()
		{
			if (!commited && !rollbacked) {
				Rollback ();
			}
		}
	}


}


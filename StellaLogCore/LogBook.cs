using System;

namespace Yavit.StellaLog.Core
{
	public class LogBook: IDisposable
	{
		readonly NestedTransactionManager transactions;

		internal readonly StellaDB.Database Database;
		public readonly LocalConfigManager LocalConfig;
		public readonly VersionController VersionController;


		public LogBook (string path)
		{
			Database = StellaDB.Database.OpenFile (path);
			transactions = new NestedTransactionManager (Database);

			using (var t = Database.BeginTransaction()) {
				LocalConfig = new LocalConfigManager (this);
				VersionController = new VersionController (this);
				t.Commit ();
			}
		}

		public void Dispose ()
		{
			transactions.Dispose ();
			Database.Dispose ();
		}


		public INestedTransaction BeginTransaction()
		{
			return transactions.BeginTransaction ();
		}
	}
}


using System;

namespace Yavit.StellaLog.Core
{
	public class LogBook: IDisposable
	{
		readonly NestedTransactionManager transactions;

		internal readonly StellaDB.Database Database;
		public readonly LocalConfigManager LocalConfig;
		public readonly VersionController VersionController;
		public readonly ConfigManager Config;
		public readonly RecordManager Records;

		public LogBook (string path)
		{
			Database = StellaDB.Database.OpenFile (path);
			transactions = new NestedTransactionManager (Database);

			using (var t = BeginTransaction()) {
				LocalConfig = new LocalConfigManager (this);
				VersionController = new VersionController (this);

				Config = new ConfigManager (this);

				Records = new RecordManager (this);

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


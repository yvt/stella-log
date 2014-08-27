using System;

namespace Yavit.StellaLog.Core
{
	public class LogBook
	{
		internal readonly StellaDB.Database Database;
		public readonly LocalConfigManager LocalConfig;
		public readonly VersionController VersionController;

		public LogBook (string path)
		{
			Database = StellaDB.Database.OpenFile (path);
			using (var t = Database.BeginTransaction()) {
				LocalConfig = new LocalConfigManager (this);
				VersionController = new VersionController (this);
				t.Commit ();
			}
		}


	}
}


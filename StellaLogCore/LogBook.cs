using System;

namespace Yavit.StellaLog.Core
{
	public class LogBook
	{
		internal readonly StellaDB.Database Database;
		public readonly LocalConfigManager LocalConfig;

		public LogBook (string path)
		{
			Database = StellaDB.Database.OpenFile (path);
			LocalConfig = new LocalConfigManager (this);
		}


	}
}


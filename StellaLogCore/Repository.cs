using System;

namespace Yavit.StellaLog.Core
{
	public class Repository
	{
		internal readonly StellaDB.Database Database;
		public readonly LocalConfigManager LocalConfig;

		public Repository (string path)
		{
			Database = StellaDB.Database.OpenFile (path);
			LocalConfig = new LocalConfigManager (this);
		}


	}
}


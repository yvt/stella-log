using System;

namespace Yavit.StellaLog.Core
{
	public class LogBook: IDisposable
	{
		readonly NestedTransactionManager transactions;

		internal readonly StellaDB.Database database;
		internal readonly LocalConfigManager localConfig;
		internal readonly VersionController versionController;
		internal readonly ConfigManager config;
		internal readonly RecordManager records;
		internal readonly ComponentManager components;

		public LogBook (string path)
		{
			database = StellaDB.Database.OpenFile (path);
			transactions = new NestedTransactionManager (database);

			using (var t = BeginTransaction()) {
				localConfig = new LocalConfigManager (this);
				versionController = new VersionController (this);

				config = new ConfigManager (this);

				records = new RecordManager (this);

				components = new ComponentManager (this);

				t.Commit ();
			}
		}

		public StellaDB.Database Database
		{
			get { return database; }
		}

		public LocalConfigManager LocalConfig
		{
			get { return localConfig; }
		}

		public VersionController VersionController
		{
			get { return versionController; }
		}

		public ConfigManager Config
		{
			get { return config; }
		}

		public RecordManager Records
		{
			get { return records; }
		}

		public ComponentManager ComponentManager
		{
			get { return components; }
		}

		public Component GetComponent(Type t)
		{
			return components.GetComponent (t);
		}

		public T GetComponent<T>() where T : Component
		{
			return components.GetComponent<T> ();
		}

		public void Dispose ()
		{
			components.UnloadAll ();
			components.DisposeAll ();
			transactions.Dispose ();
			database.Dispose ();
		}


		public INestedTransaction BeginTransaction()
		{
			return transactions.BeginTransaction ();
		}
	}
}


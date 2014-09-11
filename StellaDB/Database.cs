using System;

namespace Yavit.StellaDB
{
	public enum JournalingMode {
		None = 0,
		Memory,
		File
	}

	public class Database: IDisposable
	{
		ITransaction currentTransaction = null;
		LowLevel.LowLevelDatabase lldb;

		readonly System.IO.Stream dbStream;
		readonly System.IO.Stream journalStream;
		readonly bool closeOnDispose;

		MasterTable master;

		public Database (System.IO.Stream dbStream, System.IO.Stream journalStream,
			bool closeOnDispose)
		{
			if (dbStream == null)
				throw new ArgumentNullException ("dbStream");

			if (!dbStream.CanRead)
				throw new ArgumentException ("Database stream must be readable.");
			if (!dbStream.CanWrite)
				throw new ArgumentException ("Database stream must be writable.");
			if (!dbStream.CanSeek)
				throw new ArgumentException ("Database stream must be seekable.");
			if (journalStream != null) {
				if (!journalStream.CanRead)
					throw new ArgumentException ("Journal stream must be readable.");
				if (!journalStream.CanWrite)
					throw new ArgumentException ("Journal stream must be writable.");
				if (!journalStream.CanSeek)
					throw new ArgumentException ("Journal stream must be seekable.");
			}

			this.journalStream = journalStream;
			this.dbStream = dbStream;
			this.closeOnDispose = closeOnDispose;

			OpenDatabase ();
		}

		public void Dispose ()
		{
			if (currentTransaction != null) {
				currentTransaction.Rollback ();
			}

			if (closeOnDispose) {
				dbStream.Dispose ();
				journalStream.Dispose ();
			}
		}

		public static Database OpenFile(string path, JournalingMode journalMode = JournalingMode.File)
		{
			if (path == null)
				throw new ArgumentNullException ("path");
			if (path.Length == 0)
				throw new ArgumentException ("Path cannot be empty.", "path");

			System.IO.Stream dbStream = null, jStream = null;
			try {
				dbStream = System.IO.File.Open(path, System.IO.FileMode.OpenOrCreate,
					System.IO.FileAccess.ReadWrite, System.IO.FileShare.Read);
				switch (journalMode) {
				case JournalingMode.None: jStream = null; break;
				case JournalingMode.Memory:
					jStream = new System.IO.MemoryStream();
					break;
				case JournalingMode.File:
					jStream = new Utils.DeleteOnCloseFileStream(path + ".jourallog");
					break;
				default:
					throw new ArgumentException("journalMode");
				}
				return new Database(dbStream, jStream, true);
			} catch {
				if (dbStream != null)
					dbStream.Close ();
				if (jStream != null)
					jStream.Close ();
				throw;
			}
		}

		public static Database CreateMemoryDatabase()
		{
			return new Database (new System.IO.MemoryStream(),
				new System.IO.MemoryStream(), true);
		}

		void OpenDatabase()
		{
			UnloadAllTables ();
			if (master != null)
				master.Unload ();

			var storage = new IO.BlockFile (dbStream);
			var param = new LowLevel.LowLevelDatabaseParameters ();
			param.NumCachedBlocks = 512;
			if (journalStream != null) {
				var wal = new IO.WalBlockFile (storage, journalStream);
				lldb = new LowLevel.LowLevelDatabase (wal, param);
			} else {
				lldb = new LowLevel.LowLevelDatabase (storage, param);
			}
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

		public ITransaction BeginTransaction()
		{
			if (currentTransaction != null) {
				currentTransaction.Rollback ();
			}
			currentTransaction = new Transaction (this);
			return currentTransaction;
		}

		internal LowLevel.LowLevelDatabase LowLevelDatabase
		{
			get {
				return lldb;
			}
		}

		internal MasterTable MasterTable
		{
			get {
				if (master == null) {
					master = new MasterTable (this);
				}
				return master;
			}
		}

		Utils.WeakValueDictionary<string, Table> tables =
			new Yavit.StellaDB.Utils.WeakValueDictionary<string, Table>();

		void UnloadAllTables()
		{
			foreach (var table in tables.Values) {
				table.Unload ();
			}
		}

		public Table GetTable(string name)
		{
			Table table;

			if (!tables.TryGetValue(name, out table)) {
				table = new Table (this, name);
				DoAutoCommit ();
				tables.Add (name, table);
			}

			return table;
		}

		public Table this [string tableName]
		{
			get {
				return GetTable (tableName);
			}
		}
	}
}


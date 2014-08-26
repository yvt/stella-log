using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture]
	public class DatabaseTest
	{
		[Test]
		public void CreateOnMemory ()
		{
			Database.CreateMemoryDatabase ();
		}
		[Test, ExpectedException(typeof(ArgumentNullException))]
		public void CreateNullPath ()
		{
			Database.OpenFile (null);
		}
		[Test, ExpectedException(typeof(ArgumentException))]
		public void CreateEmptyPath ()
		{
			Database.OpenFile ("");
		}
		[Test]
		public void CreateFileNoJournal ()
		{
			using (var tmp = new TemporaryFile()) {
				Database.OpenFile(tmp.FileName, JournalingMode.None);
			}
		}
		[Test]
		public void CreateFileMemoryJournal ()
		{
			using (var tmp = new TemporaryFile()) {
				Database.OpenFile(tmp.FileName, JournalingMode.Memory);
			}
		}
		[Test]
		public void CreateFileDiskJournal ()
		{
			using (var tmp = new TemporaryFile()) {
				Database.OpenFile(tmp.FileName, JournalingMode.File);
			}
		}

		[Test]
		public void TransactionCommit ()
		{
			using (var db = Database.CreateMemoryDatabase ()) {
				using (var t = db.BeginTransaction()) {
					t.Commit ();
				}
			}
		}
		[Test]
		public void TransactionRollback ()
		{
			using (var db = Database.CreateMemoryDatabase ()) {
				using (var t = db.BeginTransaction()) {
					t.Rollback ();
				}
			}
		}
		[Test]
		public void TransactionAutoRollback ()
		{
			using (var db = Database.CreateMemoryDatabase ()) {
				using (var t = db.BeginTransaction()) {
				}
			}
		}
	}
}


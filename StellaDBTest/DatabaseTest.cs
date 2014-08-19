﻿using NUnit.Framework;
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
		public void CreateTable1()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["testtable"];
			table.AutoIncrementRowIdValue = 1; // ensure table is created
		}
		[Test]
		public void CreateTable2()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["testtabletesttabletesttabletesttabletesttabletesttabletesttabletesttable"];
			table.AutoIncrementRowIdValue = 1; // ensure table is created
		}
	}
}


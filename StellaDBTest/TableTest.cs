using NUnit.Framework;
using System;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class TableTest
	{

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

		[Test]
		public void EnsureIndex1()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.EnsureIndex (new [] {
				Table.IndexEntry.CreateNumericIndexEntry("hoge")
			});
		}

	}
}


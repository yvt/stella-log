using NUnit.Framework;
using System;
using System.Linq;
using System.Linq.Expressions;

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
		public void Drop()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["testtable"];
			table.AutoIncrementRowIdValue = 1; // ensure table is created
			table.Drop ();
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

		[Test]
		public void DeleteIndex()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.EnsureIndex (new [] {
				Table.IndexEntry.CreateNumericIndexEntry("hoge")
			});
			table.RemoveIndex (new string[] { "hoge" });
		}

		[Test]
		public void EnsureIndexAndDrop()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.EnsureIndex (new [] {
				Table.IndexEntry.CreateNumericIndexEntry("hoge")
			});
			table.Drop ();
		}

		[Test]
		public void Insert()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Insert (2, "Koiyo!", false);
			Assert.That (table.Fetch (1).ToObject (), Is.EqualTo ("Iiyo!"));
			Assert.That (table.Fetch (2).ToObject (), Is.EqualTo ("Koiyo!"));
		}
		[Test]
		public void InsertAutoIncrement()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.AutoIncrementRowIdValue = 1;
			table.Insert ("Iiyo!", false);
			table.Insert ("Koiyo!", false);
			Assert.That (table.Fetch (1).ToObject (), Is.EqualTo ("Iiyo!"));
			Assert.That (table.Fetch (2).ToObject (), Is.EqualTo ("Koiyo!"));
		}

		[Test, ExpectedException(typeof(InvalidOperationException))]
		public void InsertDuplicateFail()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Insert (1, "Koiyo!", false);
		}

		[Test]
		public void InsertDuplicateUpdate()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Insert (1, "Koiyo!", true);
			Assert.That (table.Fetch (1).ToObject (), Is.EqualTo ("Koiyo!"));
		}

		[Test, ExpectedException(typeof(InvalidOperationException))]
		public void UpdateFail()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Update (2, "Koiyo!");
		}

		[Test]
		public void Update()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Update (1, "Koiyo!");
			Assert.That (table.Fetch (1).ToObject (), Is.EqualTo ("Koiyo!"));
		}

		[Test]
		public void Delete()
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			table.Insert (1, "Iiyo!", false);
			table.Delete (1);
			Assert.That (table.Fetch (1), Is.Null);
		}

		[Test]
		public void InsertMany([Values(0, 1, 10, 100, 1000)] int count)
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];
			var items = Enumerable.Range (1, count).ToArray();
			foreach (var i in items) {
				table.Insert (i, i, false);
			}
			foreach (var i in items) {
				Assert.That (table.Fetch (i).ToObject (), Is.EqualTo (i));
			}
		}

		void QueryTestRowIdFilter(Expression<Func<long, Ston.StonVariant, bool>> pred)
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];

			var items = Enumerable.Range (1, 100).ToArray();
			foreach (var i in items) {
				table.Insert (i, i, false);
			}

			var stmt = table.Prepare (pred);
			var result = (from e in table.Query (stmt)
				select e.ToObject<int> ()).ToArray();

			var compiled = pred.Compile ();
			var expected = items.Where (v => compiled (v, null)).ToArray();

			Assert.That (result, Is.EquivalentTo (expected));
		}

		[Test] public void QueryTestRowIdFilter1([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value >= param); }
		[Test] public void QueryTestRowIdFilter2([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value > param); }
		[Test] public void QueryTestRowIdFilter3([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value <= param); }
		[Test] public void QueryTestRowIdFilter4([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value < param); }
		[Test] public void QueryTestRowIdFilter5([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value == param); }
		[Test] public void QueryTestRowIdFilter6([Range(1, 100, 17)] int param)
		{ QueryTestRowIdFilter((value, other) => value != param); }
		[Test] public void QueryTestRowIdFilter7()
		{ QueryTestRowIdFilter((value, other) => true); }
		[Test] public void QueryTestRowIdFilter8()
		{ QueryTestRowIdFilter((value, other) => false); }

		void QueryTestValueFilter(Expression<Func<long, Ston.StonVariant, bool>> pred)
		{
			var db = Database.CreateMemoryDatabase ();
			var table = db ["table"];

			var items = Enumerable.Range (1, 100).ToArray();
			foreach (var i in items) {
				table.Insert (i, i, false);
			}

			var stmt = table.Prepare (pred);
			var result = (from e in table.Query (stmt)
				select e.ToObject<int> ()).ToArray();

			var compiled = pred.Compile ();
			var expected = items.Where (v => compiled (v, new Ston.StaticStonVariant(v))).ToArray();

			Assert.That (result, Is.EquivalentTo (expected));
		}

		[Test] public void QueryTestValueFilter1([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value >= param); }
		[Test] public void QueryTestValueFilter2([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value > param); }
		[Test] public void QueryTestValueFilter3([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value <= param); }
		[Test] public void QueryTestValueFilter4([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value < param); }
		[Test] public void QueryTestValueFilter5([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value == param); }
		[Test] public void QueryTestValueFilter6([Range(1, 100, 17)] int param)
		{ QueryTestValueFilter((other, value) => value != param); }
		[Test] public void QueryTestValueFilter7()
		{ QueryTestValueFilter((other, value) => true); }
		[Test] public void QueryTestValueFilter8()
		{ QueryTestValueFilter((other, value) => false); }



	}
}


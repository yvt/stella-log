using NUnit.Framework;
using System;

namespace Yavit.StellaLog.Core.Test
{
	[TestFixture ()]
	public class RecordTest
	{
		[Test ()]
		public void RecordCreate ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.Records.CreateRecord ();
				}
			}
		}
		[Test ()]
		public void RecordSave1 ()
		{
			using (var tmp = new TemporaryLogBook()) {
				long recordId;
				using (var book = tmp.Open()) {
					var r = book.Records.CreateRecord ();
					r.Notes = "Hoge";
					r.Save ();
					recordId = (long)r.RecordId;

					r = book.Records.Fetch (recordId);
					Assert.That (r.Notes, Is.EqualTo("Hoge"));
				}
			}
		}
		[Test ()]
		public void RecordSave2 ()
		{
			using (var tmp = new TemporaryLogBook()) {
				long recordId;
				using (var book = tmp.Open()) {
					var r = book.Records.CreateRecord ();
					r.Notes = "Hoge";
					r.Save ();
					recordId = (long)r.RecordId;
				}
				using (var book = tmp.Open ()) {
					var r = book.Records.Fetch (recordId);
					Assert.That (r.Notes, Is.EqualTo("Hoge"));
				}
			}
		}

		[Test ()]
		public void RecordCustomAttribute ()
		{
			using (var tmp = new TemporaryLogBook()) {
				long recordId;
				using (var book = tmp.Open()) {
					var r = book.Records.CreateRecord ();
					r["Blah"] = "Hoge";
					r.Save ();
					recordId = (long)r.RecordId;
				}
				using (var book = tmp.Open ()) {
					var r = book.Records.Fetch (recordId);
					Assert.That (r["Blah"], Is.EqualTo("Hoge"));
				}
			}
		}
	}
}


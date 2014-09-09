using NUnit.Framework;
using System;

namespace Yavit.StellaLog.Core.Test
{
	[TestFixture]
	public class LocalConfigTest
	{
		[Test]
		public void Insert1()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.LocalConfig ["Hello"] = "hoge";
					Assert.That (book.LocalConfig ["Hello"], Is.EqualTo ("hoge"));
				}
			}
		}
		[Test]
		public void Insert2()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.LocalConfig ["Hello"] = "hoge";
				}
				using (var book = tmp.Open()) {
					Assert.That (book.LocalConfig ["Hello"], Is.EqualTo ("hoge"));
				}
			}
		}

		[Test]
		public void Overwrite1()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.LocalConfig ["Hello"] = "hoge";
					book.LocalConfig ["Hello"] = "piyo";
					Assert.That (book.LocalConfig ["Hello"], Is.EqualTo ("piyo"));
				}
			}
		}
		[Test]
		public void Overwrite2()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.LocalConfig ["Hello"] = "hoge";
					book.LocalConfig ["Hello"] = "piyo";
				}
				using (var book = tmp.Open()) {
					Assert.That (book.LocalConfig ["Hello"], Is.EqualTo ("piyo"));
				}
			}
		}
	}
}


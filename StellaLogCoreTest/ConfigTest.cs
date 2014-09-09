using NUnit.Framework;
using System;

namespace Yavit.StellaLog.Core.Test
{
	[TestFixture ()]
	public class ConfigTest
	{
		[Test]
		public void Insert1()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.Config ["Hello"] = "hoge";
					Assert.That (book.Config ["Hello"], Is.EqualTo ("hoge"));
				}
			}
		}
		[Test]
		public void Insert2()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.Config ["Hello"] = "hoge";
				}
				using (var book = tmp.Open()) {
					Assert.That (book.Config ["Hello"], Is.EqualTo ("hoge"));
				}
			}
		}

		[Test]
		public void Overwrite1()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.Config ["Hello"] = "hoge";
					book.Config ["Hello"] = "piyo";
					Assert.That (book.Config ["Hello"], Is.EqualTo ("piyo"));
				}
			}
		}
		[Test]
		public void Overwrite2()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.Config ["Hello"] = "hoge";
					book.Config ["Hello"] = "piyo";
				}
				using (var book = tmp.Open()) {
					Assert.That (book.Config ["Hello"], Is.EqualTo ("piyo"));
				}
			}
		}
	}
}


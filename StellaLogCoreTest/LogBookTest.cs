using NUnit.Framework;
using System;

namespace Yavit.StellaLog.Core.Test
{
	[TestFixture ()]
	public class LogBookTest
	{
		[Test ()]
		public void Create ()
		{
			using (var tmp = new TemporaryLogBook()){
				tmp.Open ().Dispose();
			}
		}
		[Test ()]
		public void OpenTwice ()
		{
			using (var tmp = new TemporaryLogBook()){
				tmp.Open ().Dispose();
				tmp.Open ().Dispose();
			}
		}
	}
}


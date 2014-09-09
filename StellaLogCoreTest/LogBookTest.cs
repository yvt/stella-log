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
			using (var tmp1 = new TemporaryFile())
			using (var tmp2 = new TemporaryFile(tmp1.FileName + ".journallog")){
				new LogBook (tmp1.FileName);
			}
		}
	}
}


using System;

namespace Yavit.StellaLog.Core.Test
{
	sealed class TemporaryLogBook: IDisposable
	{
		TemporaryFile tmp, tmpWalLog;
		public TemporaryLogBook ()
		{
			tmp = new TemporaryFile ();
			tmpWalLog = new TemporaryFile (tmp.FileName + ".journallog");
		}

		public LogBook Open()
		{
			return new LogBook (tmp.FileName);
		}

		public void Dispose ()
		{
			tmp.Dispose ();
			tmpWalLog.Dispose ();
		}
	}
}


using System;

namespace Yavit.StellaLog.Core
{
	public sealed class VersionController
	{
		readonly LogBook book;
		readonly StellaDB.Database db;

		internal VersionController (LogBook book)
		{
			this.book = book;
			db = book.Database;
		}

		internal byte[] CurrentRevisionRaw
		{
			get {
				return (byte[])book.LocalConfig ["StellaVCS.Revision"];
			}
			set {
				book.LocalConfig ["StellaVCS.Revision"] = value;
			}
		}

		public byte[] GetCurrentRevision()
		{
			return (byte[])CurrentRevisionRaw.Clone();
		}



	}
}


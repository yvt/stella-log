using System;

namespace Yavit.StellaLog.Core
{
	public sealed class VersionController
	{
		readonly Repository rep;
		readonly StellaDB.Database db;

		internal VersionController (Repository rep)
		{
			this.rep = rep;
			db = rep.Database;
		}

		internal byte[] CurrentRevisionRaw
		{
			get {
				return (byte[])rep.LocalConfig ["StellaVCS.Revision"];
			}
			set {
				rep.LocalConfig ["StellaVCS.Revision"] = value;
			}
		}

	}
}


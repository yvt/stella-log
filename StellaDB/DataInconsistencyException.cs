using System;

namespace Yavit.StellaDB
{
    [Serializable]
	public class DataInconsistencyException: InvalidOperationException
	{
		public DataInconsistencyException ():
		this("Inconsistency was found in the database.")
		{
		}
		public DataInconsistencyException (Exception ex):
		this("Inconsistency was found in the database.", ex)
		{
		}
		public DataInconsistencyException (string msg):
		base(msg)
		{
		}
		public DataInconsistencyException (string msg, Exception ex):
		base(msg, ex)
		{
		}
	}
}


using System;

namespace Yavit.StellaDB.Ston
{
    [Serializable]
	public class StonException: Exception
	{
		public StonException (): 
		base("Error occured while processing serialized data.")
		{
		}
		public StonException (Exception ex): 
		base("Error occured while processing serialized data.", ex)
		{
		}
		public StonException (string msg): 
		base("Error occured while processing serialized data.: " + msg)
		{
		}
		public StonException (string msg, Exception ex): 
		base("Error occured while processing serialized data.: " + msg, ex)
		{
		}
	}
}


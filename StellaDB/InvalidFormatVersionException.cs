using System;

namespace Yavit.StellaDB
{
	public class InvalidFormatVersionException: NotSupportedException
	{
		public InvalidFormatVersionException (): this("Invalid version.")
		{
		}
		public InvalidFormatVersionException (string msg): base(msg)
		{
		}
	}
}


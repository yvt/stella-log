using System;

namespace Yavit.StellaDB
{
	public class InvalidMagicNumberException: DataInconsistencyException
	{
		public InvalidMagicNumberException ():
		base("Header magic is invalid or missing.")
		{
		}
	}
}


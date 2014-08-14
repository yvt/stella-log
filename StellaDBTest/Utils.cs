using System;

namespace Yavit.StellaDB.Test
{
	static class Utils
	{
		static Random r = new Random ();
		public static byte[] GenerateRandomBytes(int num)
		{
			byte[] b = new byte[num];
			r.NextBytes (b);
			return b;
		}

	}
}


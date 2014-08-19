using System;
using System.Linq;
using System.Collections.Generic;

namespace Yavit.StellaDB
{
	public class Table
	{
		string tableName;

		internal Table (Database db, string tableName)
		{
			this.tableName = tableName;
			if (tableName == null)
				throw new ArgumentNullException ("tableName");



		}

	}
}


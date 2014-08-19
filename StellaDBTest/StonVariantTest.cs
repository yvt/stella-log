using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;
using System;
using System.Collections.Generic;
using Yavit.StellaDB.Ston;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class StonVariantTest
	{

		[Test ()]
		public void List ()
		{
			var ser = new StonSerializer ();
			var data = ser.Serialize (new object[]{ 1, 2, 3 });
			var variant = new SerializedStonVariant (new StonReader (data));
			Assert.That (variant [0].Value, Is.EqualTo (1));
			Assert.That (variant [1].Value, Is.EqualTo (2));
			Assert.That (variant [2].Value, Is.EqualTo (3));
		}
		[Test ()]
		public void Dictionary ()
		{
			var ser = new StonSerializer ();
			var data = ser.Serialize (new Dictionary<string, object> {
				{ "hoge", 1 },
				{ "piyo", 2 }
			});
			var variant = new SerializedStonVariant (new StonReader (data));
			Assert.That (variant ["hoge"].Value, Is.EqualTo (1));
			Assert.That (variant ["piyo"].Value, Is.EqualTo (2));
		}
	}
}


using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;
using System;
using Yavit.StellaDB.Ston;
using System.Collections.Generic;

namespace Yavit.StellaDB.Test
{
	[TestFixture ()]
	public class StonConverterTest
	{
		sealed class Hoge
		{ }

		class Converter: StonConverter
		{
			public override object Deserialize (IDictionary<string, object> dictionary, Type type, StonSerializer serializer)
			{
				if (dictionary.ContainsKey("hello")) {
					Assert.That (type, Is.EqualTo (typeof(Hoge)));
					return new Hoge ();
				}
				Assert.Fail ();
				return null;
			}

			public override IDictionary<string, object> Serialize (object obj, StonSerializer serializer)
			{
				if (obj is Hoge) {
					return new Dictionary<string, object> () {
						{ "hello", "world!" }
					};
				} else {
					Assert.Fail ("Object is not an instance of Hoge.");
					return null;
				}
			}

			public override IEnumerable<Type> SupportedTypes {
				get {
					return new Type[] { typeof(Hoge) };
				}
			}
		}

		StonSerializer CreateSerializer()
		{
			var s = new StonSerializer ();
			s.RegisterConverters (new StonConverter[] { new Converter() });
			return s;
		}


		[Test ()]
		public void Simple ()
		{
			var hoge = new Hoge ();
			var ser = CreateSerializer ();
			var bytes = ser.Serialize (hoge);
			var obj = ser.Deserialize<Hoge> (bytes);
			Assert.That (obj, Is.Not.Null);
		}

		[Test ()]
		public void Array ()
		{
			var ser = CreateSerializer ();
			var bytes = ser.Serialize (new Hoge[] {new Hoge(), new Hoge()});
			var obj = ser.Deserialize<IList<Hoge>> (bytes);
			Assert.That (obj, Is.Not.Null);
			Assert.That (obj.Count, Is.EqualTo (2));
			Assert.That (obj[0], Is.Not.Null);
			Assert.That (obj[1], Is.Not.Null);
		}
		[Test ()]
		public void DoubleArray ()
		{
			var ser = CreateSerializer ();
			var bytes = ser.Serialize (new Hoge[][] {new Hoge[] {new Hoge(), new Hoge()}});
			var obj = ser.Deserialize<IList<IList<Hoge>>> (bytes);
			Assert.That (obj, Is.Not.Null);
			Assert.That (obj.Count, Is.EqualTo (1));
			Assert.That (obj[0], Is.Not.Null);
			Assert.That (obj[0].Count, Is.EqualTo (2));
			Assert.That (obj[0][0], Is.Not.Null);
			Assert.That (obj[0][1], Is.Not.Null);
		}

		[Test ()]
		public void Dictionary ()
		{
			var ser = CreateSerializer ();
			var bytes = ser.Serialize (new Dictionary<string, Hoge> {
				{ "foo", new Hoge() }
			});
			var obj = ser.Deserialize<IDictionary<string, Hoge>> (bytes);
			Assert.That (obj, Is.Not.Null);
			Assert.That (obj.ContainsKey("foo"));
			Assert.That (obj["foo"], Is.Not.Null);
		}
		[Test ()]
		public void DoubleDictionary ()
		{
			var ser = CreateSerializer ();
			var bytes = ser.Serialize (new Dictionary<string, Dictionary<string, Hoge>> {
				{ "bar", new Dictionary<string, Hoge> {
					{ "foo", new Hoge() }
				}}
			});
			var obj = ser.Deserialize<IDictionary<string, IDictionary<string, Hoge>>> (bytes);
			Assert.That (obj, Is.Not.Null);
			Assert.That (obj.ContainsKey("bar"));
			Assert.That (obj["bar"], Is.Not.Null);
			Assert.That (obj["bar"].ContainsKey("foo"));
			Assert.That (obj["bar"]["foo"], Is.Not.Null);
		}
	}
}


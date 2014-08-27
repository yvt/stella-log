using NUnit.Framework;
using System;
using Yavit.StellaDB.Ston;
using System.ComponentModel;

namespace Yavit.StellaDB.Test
{
	[TestFixture]
	public class StonGeneratedConverterTest
	{
		static readonly Random r = new Random();

		[Serializable]
		public class TestClass
		{
			public int Value;

			public TestClass()
			{
				while (Value == 0) {
					Value = r.Next();
				}
			}

			public override bool Equals (object obj)
			{
				if (obj == null)
					return false;
				if (ReferenceEquals (this, obj))
					return true;
				if (obj.GetType () != typeof(TestClass))
					return false;
				TestClass other = (TestClass)obj;
				return Value == other.Value;
			}
			

			public override int GetHashCode ()
			{
				unchecked {
					return Value.GetHashCode ();
				}
			}

			public override string ToString ()
			{
				return string.Format ("[TestClass: Value={0}]", Value);
			}
			
		}

		[Serializable]
		public class DefaultedTestClassWithDefaultValue
		{
			[DefaultValue(114514)]
			public int Value = 114514;

			public DefaultedTestClassWithDefaultValue()
			{
			}

			public override bool Equals (object obj)
			{
				if (obj == null)
					return false;
				if (ReferenceEquals (this, obj))
					return true;
				if (obj.GetType () != typeof(DefaultedTestClassWithDefaultValue))
					return false;
				DefaultedTestClassWithDefaultValue other = (DefaultedTestClassWithDefaultValue)obj;
				return Value == other.Value;
			}


			public override int GetHashCode ()
			{
				unchecked {
					return Value.GetHashCode ();
				}
			}
			public override string ToString ()
			{
				return string.Format ("[DefaultedTestClassWithDefaultValue: Value={0}]", Value);
			}
			
		}

		[Serializable]
		public class TestClassWithDefaultValue
		{
			[DefaultValue(114514)]
			public int Value = 114514;

			public TestClassWithDefaultValue()
			{
				while (Value == 114514) {
					Value = r.Next();
				}
			}

			public override bool Equals (object obj)
			{
				if (obj == null)
					return false;
				if (ReferenceEquals (this, obj))
					return true;
				if (obj.GetType () != typeof(TestClassWithDefaultValue))
					return false;
				TestClassWithDefaultValue other = (TestClassWithDefaultValue)obj;
				return Value == other.Value;
			}


			public override int GetHashCode ()
			{
				unchecked {
					return Value.GetHashCode ();
				}
			}
			public override string ToString ()
			{
				return string.Format ("[TestClassWithDefaultValue: Value={0}]", Value);
			}
			
		}

		[Datapoints]
		public static readonly Type[] Types = new [] {
			typeof(TestClass),
			typeof(TestClassWithDefaultValue),
			typeof(DefaultedTestClassWithDefaultValue)
		};

		object Make(Type t)
		{
			return t.GetConstructor(new Type[]{}).Invoke(new object[]{});
		}

		[Test, Theory]
		public void Serialize(Type type)
		{
			var ser = new StonSerializer ();
			ser.Serialize (Make(type));
		}
		[Test, Theory]
		public void SerializeOptimized(Type type)
		{
			var ser = new StonSerializer ();
			for (int i = 0; i < 100; ++i) { // Trigger the optimization
				ser.Serialize (Make(type));
			}
		}
		[Test, Theory]
		public void Deserialize(Type type)
		{
			var ser = new StonSerializer ();
			var obj1 = Make(type);
			var b = ser.Serialize (obj1);
			var obj2 = ser.Deserialize (b, type);
			Assert.That (obj1, Is.EqualTo (obj2));
		}
		[Test, Theory]
		public void DeserializeOptimized(Type type)
		{
			var ser = new StonSerializer ();
			var obj1 = Make(type);
			var b = ser.Serialize (obj1);
			for (int i = 0; i < 100; ++i) { // Trigger the optimization
				var obj2 = ser.Deserialize (b, type);
				Assert.That (obj1, Is.EqualTo (obj2));
			}
		}
	}
}


using NUnit.Framework;
using System;

namespace Yavit.StellaLog.Core.Test
{
	[TestFixture ()]
	public class ComponentTest
	{
		public sealed class TestComponent: StellaLog.Core.Component 
		{
			public bool Loaded = false;
			public TestComponent(LogBook book)
			{

			}
			public override void Load ()
			{
				Loaded = true;
			}

			public override void Unload ()
			{
				Loaded = false;
			}

			public override string ToString ()
			{
				return "Hey! I'm TestComponent! Still alive!";
			}
		}
		[Test ()]
		public void Load1 ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var c = book.ComponentManager.AddComponent (typeof(TestComponent));
					Assert.That (book.GetComponent<TestComponent> ().Loaded);
				}
			}
		}
		[Test ()]
		public void Load2 ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var c = book.ComponentManager.AddComponent (typeof(TestComponent));
				}
				using (var book = tmp.Open()) {
					Assert.That (book.GetComponent<TestComponent> ().Loaded);
				}
			}
		}
		[Test ()]
		public void Unload ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var c = book.ComponentManager.AddComponent (typeof(TestComponent));
					var obj = (TestComponent)c.Object;
					Assert.That (book.GetComponent<TestComponent> ().Loaded);

					book.ComponentManager.RemoveComponent (c);
					Assert.That (book.GetComponent<TestComponent> (), Is.Null);
					Assert.That (!obj.Loaded);
				}
			}
		}
		[Test ()]
		public void UnloadByVCS ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var c = book.ComponentManager.AddComponent (typeof(TestComponent));
					var obj = (TestComponent)c.Object;
					Assert.That (book.GetComponent<TestComponent> ().Loaded);

					book.VersionController.RevertLocalModifications ();

					Assert.That (book.GetComponent<TestComponent> (), Is.Null);
					Assert.That (!obj.Loaded);
				}
			}
		}
	}
}


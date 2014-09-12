using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaLog.Core
{
	public sealed class ComponentManager
	{
		readonly LogBook book;
		readonly VersionControlledTable table;

		[Serializable]
		sealed class DbComponent
		{
			public string Name;
		}

		readonly Dictionary<Type, ComponentInfo> components = 
			new Dictionary<Type, ComponentInfo>();

		internal ComponentManager(LogBook book)
		{
			this.book = book;

			table = book.versionController.GetTable ("Components");

			new VersionControlledTableBufferedEventProxy(table).Updated += (sender, e) => {
				if (e.Reason != VersionControlledTableUpdateReason.VersionController) {
					return;
				}
				foreach (var c in components) {
					if (c.Value.RowId == e.RowId) {
						var obj = c.Value.Object;
						c.Value.Unload();
						obj.Dispose();
						components.Remove(c.Key);

						if (e.newValue.Length > 0) {
							var nc = new ComponentInfo(e.RowId, book,
								table.BaseTable.Serializer.Deserialize<DbComponent>(e.newValue).Name);
							if (nc.Object != null) {
								components.Add(nc.Object.GetType(), nc);

								try {
									nc.Load();
								} catch {
									components.Remove(nc.Object.GetType());
									nc.Object.Dispose();
									throw;
								}
							}
						}
						break;
					}
				}
			};

			foreach (var row in table.Query(table.Prepare((r,v)=>true))) {
				var e = row.ToObject<DbComponent> ();
				try {
					AddComponent(e.Name);
				} catch (ComponentLoadException) {
					// Ignore load error...
				}
			}
		}

		public ComponentInfo AddComponent(string name)
		{
			// Find existing
			foreach (var e in components) {
				if (e.Value.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase)) {
					return e.Value;
				}
			}

			using (var t = book.BeginTransaction()) {
				// Add to table
				var rowId = table.Insert (new DbComponent () {
					Name = name
				});

				ComponentInfo c;
				try {
					// Create component
					c = new ComponentInfo (rowId, book, name);
					if (c.Object == null) {
						throw new ComponentLoadException (c.LoadException);
					}
					components.Add(c.Object.GetType(), c);
					t.Commit ();
				} catch {
					table.Delete (rowId);
					t.Commit ();
					throw;
				}

				if (c.Object == null) {
					throw new ComponentLoadException (c.LoadException);
				}

				try {
					c.Load();
					return c;
				} catch {
					using (var t2 = book.BeginTransaction()) {
						table.Delete (c.RowId);
						components.Remove (c.Object.GetType());
						t2.Commit ();
					}
					c.Object.Dispose ();
					throw;
				}

			}
		}

		public void RemoveComponent(ComponentInfo info)
		{
			foreach (var e in components) {
				if (e.Value == info) {
					var obj = info.Object;
					info.Unload ();
					using (var t = book.BeginTransaction()) {
						table.Delete (info.RowId);
						components.Remove (e.Key);
						t.Commit ();
					}
					obj.Dispose ();
					break;
				}
			}
		}

		public ComponentInfo AddComponent(Type type)
		{
			return AddComponent (type.AssemblyQualifiedName);
		}

		public ComponentInfo GetComponentInfo(Type type)
		{
			ComponentInfo info;
			if (components.TryGetValue(type, out info)) {
				return info;
			}
			return null;
		}

		public Component GetComponent(Type t)
		{
			var info = GetComponentInfo (t);
			return info != null ? info.Object : null;
		}

		public T GetComponent<T>() where T : Component
		{
			return (T)GetComponent (typeof(T));
		}

		IEnumerable<Component> disposedObjects;

		internal void UnloadAll()
		{
			var objs = from e in components
				select e.Value.Object;
			disposedObjects = objs.ToArray();

			foreach (var e in components) {
				e.Value.Object.Unload ();
			}

			components.Clear ();
		}

		internal void DisposeAll()
		{
			foreach (var e in disposedObjects) {
				e.Dispose ();
			}
		}
	}

	public class ComponentLoadException: Exception
	{
		public ComponentLoadException(Exception ex):
		base("Error occured while loading a component.", ex)
		{}
	}

	public class Component: MarshalByRefObject, IDisposable
	{
		public virtual void Load() { }
		public virtual void Unload() { }
		public void Dispose ()
		{
		}
	}

	public sealed class ComponentInfo
	{
		internal long RowId;
		public LogBook LogBook { get; private set; }
		public string Name { get; private set; }
		public Component Object { get; private set; }
		public Exception LoadException { get; private set; }

		internal ComponentInfo(long rowId, LogBook book, string name)
		{
			this.RowId = rowId;
			LogBook = book;
			Name = name;
			try {
				var t = Type.GetType(Name, true);
				if (!typeof(Component).IsAssignableFrom(t)) {
					throw new InvalidOperationException(string.Format(
						"Type {0} is not a component.", name));
				}
				var ctor = t.GetConstructor(new Type[] {typeof(LogBook)});
				if (ctor == null) {
					throw new InvalidOperationException(string.Format(
						"Component {0} doesn't provide an appropriate constructor.", name));
				}
				Object = (Component)ctor.Invoke(new object[] {book});
			} catch (Exception ex) {
				LoadException = ex;
			}
		}

		internal void Load()
		{
			if (Object != null) {
				Object.Load ();
			}
		}

		internal void Unload()
		{
			if (Object != null) {
				Object.Unload ();
			}
			Object = null;
		}
	}
}


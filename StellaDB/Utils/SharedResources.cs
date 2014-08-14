using System;
using System.Collections.Generic;

namespace Yavit.StellaDB.Utils
{
	internal abstract class SharedResourceManager<T, TKey, TCategory>
		where TKey : struct
		where TCategory : struct
		where T : SharedResource<TKey, TCategory>

	{
		readonly Dictionary<TCategory, Item> Categories = new Dictionary<TCategory, Item>();
		readonly Dictionary<TKey, Item> Items = new Dictionary<TKey, Item>();

		sealed class Item
		{
			public readonly SharedResourceManager<T, TKey, TCategory> Manager;
			public readonly T Resource;
			public SharedResourceRef<T, TKey, TCategory> FirstOwner;
			TCategory? category;

			public Item NextSameCateogry;
			public Item PrevSameCateogry;

			public Item(SharedResourceManager<T, TKey, TCategory> manager, T resource)
			{
				this.Manager = manager;
				this.Resource = resource;
			}

			public void AddOwner(SharedResourceRef<T, TKey, TCategory> ow)
			{
				if (FirstOwner == null) {
					FirstOwner = ow;
					ow.NextOwner = ow;
					ow.PrevOwner = ow;
					return;
				}
				if (FirstOwner.PrevOwner == null ||
					FirstOwner.NextOwner == null) {
					throw new InvalidOperationException ();
				}
				ow.NextOwner = FirstOwner.NextOwner;
				ow.PrevOwner = FirstOwner;
				FirstOwner.NextOwner = ow;
				ow.NextOwner.PrevOwner = ow;
				if (ow.PrevOwner == null ||
					ow.NextOwner == null) {
					throw new InvalidOperationException ();
				}
			}

			public TCategory? Category
			{
				get {
					return category;
				}
				set {
					if (category == null && value == null) {
						return;
					}
					IEqualityComparer<TCategory> comparer = EqualityComparer<TCategory>.Default;
					if (category != null &&
						value != null && 
						comparer.Equals((TCategory)category, (TCategory)value)) {
						return;
					}
					UnlinkSameCategory ();
					category = value;
					if (category != null) {
						LinkToSameCateogry ();
					}
				}
			}
				
			public void LinkToSameCateogry()
			{
				UnlinkSameCategory ();

				if (Category == null) {
					return;
				}

				Item catFirst;
				if (Manager.Categories.TryGetValue((TCategory)Category, out catFirst)) {
					PrevSameCateogry = catFirst;
					NextSameCateogry = catFirst.NextSameCateogry;
					catFirst.NextSameCateogry = this;
					NextSameCateogry.PrevSameCateogry = this;
				} else {
					PrevSameCateogry = NextSameCateogry = this;
					Manager.Categories.Add ((TCategory)Category, this);
				}
			}

			public void UnlinkSameCategory()
			{
				if (NextSameCateogry == null) {
					return;
				}
				if (NextSameCateogry == this) {
					NextSameCateogry = null;
					PrevSameCateogry = null;
					Manager.Categories.Remove ((TCategory) Category);
					return;
				}

				if (Manager.Categories[(TCategory)Category] == this) {
					Manager.Categories [(TCategory)Category] = NextSameCateogry;
				}
				NextSameCateogry.PrevSameCateogry = PrevSameCateogry;
				PrevSameCateogry.NextSameCateogry = NextSameCateogry;
			}
		}

		protected SharedResourceManager ()
		{
		}

		protected abstract T CreateResource();

		public void Load(SharedResourceRef<T, TKey, TCategory> r, ref T item, 
			TKey id, TCategory category, bool initialize)
		{
			IEqualityComparer<TKey> comparer = EqualityComparer<TKey>.Default;
			if (item != null && 
				comparer.Equals((TKey)item.ResourceId, id)) {
				return;
			}

			Item newItem;
			if (Items.TryGetValue(id, out newItem)) {
				// Already loaded
				Unload (r, ref item);
				item = newItem.Resource;
			} else {
				// If it's unique referece, we might be able to avoid reallocation
				if (item != null && r.IsUniqueOwner) {
					TKey oldId = (TKey)item.ResourceId;
					var itm = Items [oldId];
					item.Unload ();
					Items.Remove (oldId);

					try {
						if (initialize)
							item.Initialize (id, category);
						else
							item.Load (id, category);
					} catch {
						itm.Category = null;
						if (item.ResourceId != null) {
							throw new InvalidOperationException ();
						}
						item = null;
						throw;
					}

					itm.Category = category;

					if (!comparer.Equals(id, (TKey)item.ResourceId)) {
						throw new InvalidOperationException ();
					}

					Items.Add (id, itm);

					return;
				}

				Unload (r, ref item);

				// Load resource
				var res = CreateResource ();
				if (initialize)
					res.Initialize (id, category);
				else
					res.Load (id, category);

				newItem = new Item (this, res);
				Items.Add (id, newItem);

				newItem.Category = category;
				item = res;
			}
			newItem.AddOwner (r);

			if (!comparer.Equals(id, (TKey)item.ResourceId)) {
				throw new InvalidOperationException ();
			}
		}

		public void InvalidateAllReferences(TKey id)
		{
			foreach (var owner in GetReferencesOfResource(id)) {
				owner.Unload ();
			}
		}

		public IEnumerable<SharedResourceRef<T, TKey, TCategory>> GetReferencesOfResource(TKey id)
		{
			Item newItem;
			if (Items.TryGetValue(id, out newItem)) {
				var owner = newItem.FirstOwner;
				var first = owner;
				do {
					var n = owner.NextOwner;
					yield return owner;
					if (n == owner) {
						break;
					}
					owner = n;
				} while (owner != first);
			}
		}

		public IEnumerable<T> Resources
		{
			get {
				foreach (var cat in Categories.Values) {
					var owner = cat;
					var first = owner;
					do {
						var n = owner.NextSameCateogry;
						yield return owner.Resource;
						if (n == owner) {
							break;
						}
						owner = n;
					} while (owner != first);
				}
			}
		}

		public IEnumerable<T> GetResourcesOfCategory(TCategory cat)
		{
			Item newItem;
			if (Categories.TryGetValue(cat, out newItem)) {
				var owner = newItem;
				var first = owner;
				do {
					var n = owner.NextSameCateogry;
					yield return owner.Resource;
					if (n == owner) {
						break;
					}
					owner = n;
				} while (owner != first);
			}
		}

		static void Unlink(SharedResourceRef<T, TKey, TCategory> r)
		{
			if (r.PrevOwner == null ||
				r.NextOwner == null) {
				throw new InvalidOperationException ();
			}
			if (!r.IsUniqueOwner) {
				r.PrevOwner.NextOwner = r.NextOwner;
				r.NextOwner.PrevOwner = r.PrevOwner;
				if (r.PrevOwner.NextOwner == null) {
					throw new InvalidOperationException ();
				}
				if (r.PrevOwner.PrevOwner == null) {
					throw new InvalidOperationException ();
				}
				if (r.NextOwner.PrevOwner == null) {
					throw new InvalidOperationException ();
				}
				if (r.NextOwner.NextOwner == null) {
					throw new InvalidOperationException ();
				}
			}
			r.PrevOwner = null;
			r.NextOwner = null;
		}

		public void Unload(SharedResourceRef<T, TKey, TCategory> r, ref T item)
		{
			if (item == null) {
				return;
			}

			var id = (TKey)item.ResourceId;
			var itm = Items [id];
			if (r.IsUniqueOwner) {
				Unlink (r);

				itm.FirstOwner = null;
				itm.Category = null;

				Items.Remove (id);
				item.Unload ();
			} else {
				if (itm.FirstOwner == r) {
					itm.FirstOwner = r.NextOwner;
				}
				Unlink (r);
			}

			item = null;
		}
	}

	internal abstract class SharedResource<TKey, TCategory>
		where TKey : struct
		where TCategory : struct
	{
		public abstract void Load(TKey id, TCategory category);
		public abstract void Initialize(TKey id, TCategory category);
		public abstract void Unload();

		public abstract TKey? ResourceId { get; }
	}

	internal class SharedResourceRef<T, TKey, TCategory> : IDisposable 
		where T : SharedResource<TKey, TCategory>
		where TKey : struct
		where TCategory : struct
	{
		private SharedResourceManager<T, TKey, TCategory> manager;
		T resource = null;
		public T Resource { get { return resource; } }

		public SharedResourceRef<T, TKey, TCategory> NextOwner;
		public SharedResourceRef<T, TKey, TCategory> PrevOwner;

		public SharedResourceRef(SharedResourceManager<T, TKey, TCategory> manager)
		{
			if (manager == null)
				throw new ArgumentNullException ("manager");

			this.manager = manager;
		}

		public bool IsUniqueOwner {
			get { return NextOwner == this; }
		}

		public void Load(TKey id, TCategory category)
		{
			IEqualityComparer<TKey> comparer = EqualityComparer<TKey>.Default;
			if (resource != null && comparer.Equals((TKey)resource.ResourceId, id)) {
				return;
			}
			manager.Load (this, ref resource, id, category, false);
		}
		public void InitializeAndLoad(TKey id, TCategory category)
		{
			IEqualityComparer<TKey> comparer = EqualityComparer<TKey>.Default;
			if (resource != null && comparer.Equals((TKey)resource.ResourceId, id)) {
				// FIXME: initialize again?
				return;
			}
			manager.Load (this, ref resource, id, category, true);
		}

		public void Unload ()
		{
			if (resource == null) {
				return;
			}
			manager.Unload (this, ref resource);
		}

		public void Dispose ()
		{
			Unload ();
		}
	}
}


using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaLog.Core.Utils
{
	sealed class PriorityQueue<TKey, TValue>
	{
		// TODO: Use heap for priority queue
		readonly SortedDictionary<TKey, TValue> dic;

		public PriorityQueue ()
		{
			dic = new SortedDictionary<TKey, TValue> ();
		}

		public void Enqueue(TKey key, TValue value)
		{
			dic.Add(key, value);
		}

		public KeyValuePair<TKey, TValue> Peek()
		{
			if (dic.Count == 0) {
				throw new InvalidOperationException ();
			}
			return dic.First ();
		}

		public KeyValuePair<TKey, TValue> Dequeue()
		{
			var e = Peek ();
			dic.Remove (e.Key);
			return e;
		}

		public int Count
		{
			get {
				return dic.Count;
			}
		}
	}
}


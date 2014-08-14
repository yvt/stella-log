using System;
using System.Collections.Generic;
using System.IO;

namespace Yavit.StellaDB.LowLevel
{
	class BufferPool
	{
		readonly int blockSize;
		readonly LinkedList<Item> buffers = new LinkedList<Item>();

		public BufferPool (int blockSize)
		{
			if (blockSize < 0) {
				throw new ArgumentOutOfRangeException("blockSize");
			}
			this.blockSize = blockSize;
		}

		public BufferHandle CreateHandle()
		{
			return new BufferHandle (this);
		}

		class Item
		{
			public readonly MemoryStream Stream;
			public readonly BinaryWriter Writer;
			public readonly BinaryReader Reader;
			public Item(int size)
			{
				Stream = new MemoryStream(new byte[size], 0, size, true, true);
				Writer = new BinaryWriter(Stream);
				Reader = new BinaryReader(Stream);
			}
		}

		public struct BufferHandle: IDisposable
		{
			public readonly BufferPool Pool;
			byte[] buffer;
			Item item;

			private LinkedListNode<Item> node;

			internal BufferHandle(BufferPool pool)
			{
				Pool = pool;
				if (pool.buffers.Count == 0) {
					node = new LinkedListNode<Item>(new Item(pool.blockSize));
				} else {
					node = pool.buffers.First;
					pool.buffers.RemoveFirst();
				}
				item = node.Value;
				buffer = item.Stream.GetBuffer();
			}

			public byte[] Buffer
			{
				get {
					return buffer;
				}
			}

			public MemoryStream Stream
			{
				get {
					return item.Stream;
				}
			}

			public BinaryReader BinaryReader
			{
				get {
					return item.Reader;
				}
			}

			public BinaryWriter BinaryWriter
			{
				get {
					return item.Writer;
				}
			}

			public void Dispose ()
			{
				if (node != null) {
					Pool.buffers.AddFirst (node);
					if (Pool.buffers.Count > 256) {
						throw new InvalidOperationException ("Buffer pool overflow. Memory leak possible.");
					}

					buffer = null;
					item = null;
					node = null;
				}
			}
		}
	}
}


using System;

namespace Yavit.StellaDB.LowLevel
{
	public class Blob: MarshalByRefObject
	{
		readonly LowLevelDatabase db;
		readonly LinkedListBlob lblob;

		internal Blob (LowLevelDatabase db, long blockId)
		{
			this.db = db;
			lblob = new LinkedListBlob (db, blockId);
		}

		public LowLevelDatabase Database {
			get {
				return db;
			}
		}

		public long BlockId
		{
			get {
				var r = lblob.BlockId;
				if (r == 0) {
					throw new ObjectDisposedException ("Blob");
				}
				return r;
			}
		}

		public long Length
		{
			get {
				using (var s = OpenStream()) {
					return s.Length;
				}
			}
		}

		public byte[] ReadAllBytes()
		{
			using (var s = OpenStream())
			using (var ms = new System.IO.MemoryStream())
			using (var h = db.BufferPool.CreateHandle()) {
				while (true) {
					int count = s.Read (h.Buffer, 0, h.Buffer.Length);
					if (count == 0) {
						break;
					}
					ms.Write (h.Buffer, 0, count);
				}
				return ms.ToArray ();
			}
		}

		public void WriteAllBytes(byte[] b)
		{
			using (var s = OpenStream()) {
				s.Write (b, 0, b.Length);
				s.SetLength (b.Length);
			}
		}

		public System.IO.Stream OpenStream()
		{
			return lblob.OpenStream();
		}

		public void Drop()
		{
			lblob.Drop();
		}
	}
}


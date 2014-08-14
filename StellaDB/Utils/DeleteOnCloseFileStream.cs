using System;
using System.IO;

namespace Yavit.StellaDB.Utils
{
	public class DeleteOnCloseFileStream: Stream
	{
		string fileName;
		Stream baseStream;

		public DeleteOnCloseFileStream (string fileName)
		{
			if (fileName == null) {
				throw new ArgumentNullException("fileName");
			}
			this.fileName = fileName;
			baseStream = File.Open(fileName, FileMode.OpenOrCreate,
				FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
		}

		protected override void Dispose (bool disposing)
		{
			if (baseStream != null) {
				baseStream.Dispose ();
				baseStream = null;
				try {
					File.Delete (fileName);
				} catch {
				}
			}
			base.Dispose (disposing);
		}

		public override void Flush ()
		{
			baseStream.Flush ();
		}

		public override int Read (byte[] buffer, int offset, int count)
		{
			return baseStream.Read (buffer, offset, count);
		}

		public override long Seek (long offset, SeekOrigin origin)
		{
			return baseStream.Seek (offset, origin);
		}

		public override void SetLength (long value)
		{
			baseStream.SetLength (value);
		}

		public override void Write (byte[] buffer, int offset, int count)
		{
			baseStream.Write (buffer, offset, count);
		}

		public override bool CanRead {
			get {
				return baseStream.CanRead;
			}
		}

		public override bool CanSeek {
			get {
				return baseStream.CanSeek;
			}
		}

		public override bool CanWrite {
			get {
				return baseStream.CanWrite;
			}
		}

		public override long Length {
			get {
				return baseStream.Length;
			}
		}

		public override long Position {
			get {
				return baseStream.Position;
			}
			set {
				baseStream.Position = value;
			}
		}

	}
}


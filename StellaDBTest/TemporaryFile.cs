
using System;
using System.IO;

namespace Yavit.StellaDB.Test
{
	public class TemporaryFile: IDisposable
	{
		private string fileName;
		public TemporaryFile (): this(Path.GetTempFileName())
		{
		}

		public TemporaryFile (string fileName)
		{
			this.fileName = fileName;
		}

		public string FileName 
		{
			get 
			{
				return fileName;
			}
		}

		public FileStream Open()
		{
			return File.Open (fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite,
				FileShare.Read | FileShare.Delete);
		}

		public void Dispose ()
		{
			if (fileName != null) {
				File.Delete (fileName);
				fileName = null;
			}
		}
	}
}


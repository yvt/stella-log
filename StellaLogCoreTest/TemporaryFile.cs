using System;
using System.IO;

namespace Yavit.StellaLog.Core.Test
{
	sealed class TemporaryFile: IDisposable
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
				try
				{
					File.Delete(fileName);
				}
				catch { }
				fileName = null;
			}
		}
	}
}


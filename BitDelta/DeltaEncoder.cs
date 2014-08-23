using System;

namespace Yavit.BitDelta
{
	public class DeltaEncoder
	{
		const byte HeaderDifferenceFlag = 0x80;

		// Used when Y unique part is shorter than one of X
		const byte HeaderSubtractFlag = 0x80; 

		const byte HeaderCommonDataLength32 = 0x7f;
		const byte HeaderCommonDataLength24 = 0x7e;
		const byte HeaderCommonDataLength16 = 0x7d;
		const byte HeaderCommonDataLength8 = 0x7c;

		readonly object syncObj = new object();
		readonly DynamicBuffer outputBuffer = new DynamicBuffer();

		int xUniqueStart, xUniqueLen;
		int yUniqueStart, yUniqueLen;

		int commonLen;

		byte[] xInput, yInput;

		#region Encoder
		public byte[] Encode(byte[] x, byte[] y)
		{
			lock (syncObj) {
				xInput = x;
				yInput = y;

				if (x.Length == y.Length) {
					// Identical?
					if (ArrayEquals(x, y)) {
						return new byte[0];
					}
				}

				outputBuffer.Length = 0;
				commonLen = 0;
				xUniqueLen = 0;
				yUniqueLen = 0;
				xUniqueStart = 0;
				yUniqueStart = 0;

				InternalEncode (0, x.Length, 0, y.Length);

				FinalizeCommonPart ();
				FinalizeUniquePart ();

				xInput = null;
				yInput = null;

				return outputBuffer.ToArray();
			}
		}

		static bool ArrayEquals(byte[] x, byte[] y)
		{
			for (int i = 0; i < x.Length; ++i) {
				if (x [i] != y [i])
					return false;
			}
			return true;
		}

		void WriteHeader(int flag, int len)
		{
			if (len < HeaderCommonDataLength8) {
				outputBuffer.Write ((byte)(flag | len));
				return;
			}
			len -= HeaderCommonDataLength8;
			if (len < 0x100) {
				outputBuffer.Write ((byte)(flag | HeaderCommonDataLength8));
				outputBuffer.Write ((byte)len);
				return;
			}
			len -= 0x100;
			if (len < 0x100) {
				outputBuffer.Write ((byte)(flag | HeaderCommonDataLength16));
				outputBuffer.Write ((byte)len);
				outputBuffer.Write ((byte)(len >> 8));
				return;
			}
			len -= 0x10000;
			if (len < 0x100) {
				outputBuffer.Write ((byte)(flag | HeaderCommonDataLength24));
				outputBuffer.Write ((byte)len);
				outputBuffer.Write ((byte)(len >> 8));
				outputBuffer.Write ((byte)(len >> 16));
				return;
			}
			len -= 0x1000000;
			{
				outputBuffer.Write ((byte)(flag | HeaderCommonDataLength32));
				outputBuffer.Write ((byte)len);
				outputBuffer.Write ((byte)(len >> 8));
				outputBuffer.Write ((byte)(len >> 16));
				outputBuffer.Write ((byte)(len >> 24));
			}
		}

		void FinalizeUniquePart()
		{
			if (xUniqueLen == 0 &&
				yUniqueLen == 0) {
				return;
			}

			WriteHeader (HeaderDifferenceFlag, xUniqueLen);
			outputBuffer.Write (xInput, xUniqueStart, xUniqueLen);
			if (yUniqueLen < xUniqueLen) {
				WriteHeader (HeaderSubtractFlag, xUniqueLen - yUniqueLen);
			} else {
				WriteHeader (0, yUniqueLen - xUniqueLen);
			}
			outputBuffer.Write (yInput, yUniqueStart, yUniqueLen);

			xUniqueStart += xUniqueLen;
			yUniqueStart += yUniqueLen;
			xUniqueLen = 0;
			yUniqueLen = 0;
		}

		void FinalizeCommonPart()
		{
			if (commonLen == 0) {
				return;
			} else if (commonLen < 3) { // empirical; find better value...
				// too short common part. Treat it as unique part
				xUniqueLen += commonLen;
				yUniqueLen += commonLen;
				commonLen = 0;
			} else {
				FinalizeUniquePart ();
				WriteHeader (0, commonLen);
				xUniqueStart += commonLen;
				yUniqueStart += commonLen;
				commonLen = 0;
			}
		}

		void EmitUniqueX(int len)
		{
			if (len == 0) {
				return;
			}
			FinalizeCommonPart ();
			xUniqueLen += len;
		}

		void EmitUniqueY(int len)
		{
			if (len == 0) {
				return;
			}
			FinalizeCommonPart ();
			yUniqueLen += len;
		}

		void EmitCommon(int len)
		{
			commonLen += len;
		}

		int[] nwscore;
		int[] nwscoreRev;
		int[] nwscoreTmp;

		void FillNeedlemanWunschScore(int xoff, int xlen, int yoff, int ylen)
		{
			int[] score = this.nwscore;
			int[] tmp = this.nwscoreTmp;
			byte[] x = xInput, y = yInput;
			score [0] = 0;
			for (int i = 0; i < ylen; ++i) {
				score [i + 1] = score [i] - 1;
			}
			for (int i = 0; i < xlen; ++i) {
				tmp [0] = score [0] - 1;
				for (int j = 0; j < ylen; ++j) {
					var sub = score [j];
					var del = score [j + 1] - 1;
					var ins = tmp [j] - 1;
					if (x[xoff + i] == y[yoff + j]) {
						// sub += 0;
					} else {
						sub -= 2; // delete + insertion
					}

					var finalScore = sub > del ? sub : del;
					finalScore = ins > finalScore ? ins : finalScore;
					tmp[j + 1] = finalScore;
				}
				Buffer.BlockCopy(tmp, 0, score, 0, ylen * 4 + 4);
			}
		}

		void FillNeedlemanWunschScoreRev(int xoff, int xlen, int yoff, int ylen)
		{
			int[] score = this.nwscoreRev;
			int[] tmp = this.nwscoreTmp;
			byte[] x = xInput, y = yInput;

			xoff += xlen - 1;
			yoff += ylen - 1;

			score [0] = 0;
			for (int i = 0; i < ylen; ++i) {
				score [i + 1] = score [i] - 1;
			}
			for (int i = 0; i < xlen; ++i) {
				tmp [0] = score [0] - 1;
				for (int j = 0; j < ylen; ++j) {
					var sub = score [j];
					var del = score [j + 1] - 1;
					var ins = tmp [j] - 1;
					if (x[xoff - i] == y[yoff - j]) {
						// sub += 0;
					} else {
						sub -= 2; // delete + insertion
					}

					var finalScore = sub > del ? sub : del;
					finalScore = ins > finalScore ? ins : finalScore;
					tmp[j + 1] = finalScore;
				}
				Buffer.BlockCopy(tmp, 0, score, 0, ylen * 4 + 4);
			}
		}

		// Hirschberg's algorithm implementation
		void InternalEncode(
			int xoff, int xlen,
			int yoff, int ylen)
		{
			if (xlen == 0 && ylen == 0) {
				return;
			} else if (xlen == 0) {
				EmitUniqueY (ylen);
			} else if (ylen == 0) {
				EmitUniqueX (xlen);
			} else if (xlen == 1 && ylen == 1) {
				byte x = xInput [xoff];
				byte y = yInput [yoff];
				if (x == y) {
					EmitCommon (1);
				} else {
					EmitUniqueX (1);
					EmitUniqueY (1);
				}
			} else if (xlen == 1) {
				// Needleman-Wunsch
				byte b = xInput [xoff];
				byte[] other = yInput;
				for (int i = 0; i < ylen; ++i) {
					if (other[i + yoff] == b) {
						EmitUniqueY (i);
						EmitCommon (1);
						EmitUniqueY (ylen - i - 1);
						return;
					}
				}
				EmitUniqueX (xlen);
				EmitUniqueY (ylen);
			} else if (ylen == 1) {
				// Needleman-Wunsch
				byte b = yInput [yoff];
				byte[] other = xInput;
				for (int i = 0; i < xlen; ++i) {
					if (other[i + xoff] == b) {
						EmitUniqueX (i);
						EmitCommon (1);
						EmitUniqueX (xlen - i - 1);
						return;
					}
				}
				EmitUniqueX (xlen);
				EmitUniqueY (ylen);
			} else {
				if (nwscore == null || ylen + 1 > nwscore.Length) {
					nwscore = new int[ylen + 1];
					nwscoreRev = new int[ylen + 1];
					nwscoreTmp = new int[ylen + 1];
				}

				int xmid = xlen >> 1;
				FillNeedlemanWunschScore (xoff, xmid, yoff, ylen);
				FillNeedlemanWunschScoreRev (xoff + xmid, xlen - xmid, yoff, ylen);

				int ymid = 0, bestscore = int.MinValue;
				int[] scoreFwd = nwscore, scoreRev = nwscoreRev;
				for (int i = 0; i <= ylen; ++i) {
					int score = scoreFwd[i] + scoreRev[ylen - i];
					if (score > bestscore) {
						ymid = i; bestscore = score;
					}
				}

				InternalEncode (xoff, xmid, yoff, ymid);
				InternalEncode (xoff + xmid, xlen - xmid, 
					yoff + ymid, ylen - ymid);
			}
		}

		#endregion

		#region Decoder

		int ReadHeader(byte[] input, ref int idx, out bool outFlag)
		{
			int index = idx;
			int b = input [index++];
			outFlag = (b & 0x80) != 0;

			int len = b & 0x7f;
			if (len == HeaderCommonDataLength8) {
				len = input [index++];
				len += HeaderCommonDataLength8;
			} else if (len == HeaderCommonDataLength16) {
				len = input [index++];
				len += input [index++] << 8;
				len += HeaderCommonDataLength8 + 0x100;
			} else if (len == HeaderCommonDataLength24) {
				len = input [index++];
				len += input [index++] << 8;
				len += input [index++] << 16;
				len += HeaderCommonDataLength8 + 0x10100;
			} else if (len == HeaderCommonDataLength32) {
				len = input [index++];
				len += input [index++] << 8;
				len += input [index++] << 16;
				len += input [index++] << 24;
				len += HeaderCommonDataLength8 + 0x1010100;
			}

			idx = index;
			return len;
		}

		byte[] InternalDecode(byte[] delta, byte[] input, bool decodeY)
		{
			if (delta.Length == 0) {
				// identical.
				return (byte[])input.Clone ();
			}
			lock (syncObj) {
				outputBuffer.Length = 0;
				int index = 0;
				int inputIndex = 0;

				while (index < delta.Length) {
					bool differentFlag;
					int len1 = ReadHeader (delta, ref index, out differentFlag);
					if (differentFlag) {
						// X
						if (decodeY) {
							inputIndex += len1;
						} else {
							outputBuffer.Write (delta, index, len1);
						}
						index += len1;

						// Y
						bool lessFlag;
						int len2 = ReadHeader (delta, ref index, out lessFlag);
						len2 = lessFlag ? len1 - len2 : len1 + len2;
						if (len2 < 0) {
							throw new System.IO.InvalidDataException ();
						}

						if (decodeY) {
							outputBuffer.Write (delta, index, len2);
						} else {
							inputIndex += len2;
						}
						index += len2;
					} else {
						// common part
						outputBuffer.Write (input, inputIndex, len1);
						inputIndex += len1;
					}
				}

				return outputBuffer.ToArray ();
			}
		}

		public byte[] DecodeX(byte[] delta, byte[] y)
		{
			return InternalDecode (delta, y, false);
		}

		public byte[] DecodeY(byte[] delta, byte[] x)
		{
			return InternalDecode (delta, x, true);
		}

		#endregion
	}
}


﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace Yavit.StellaDB.Indexer
{
	sealed class IndexParameters
	{
		public class Field
		{
			public string Name;
			public object Parameters;
		}
		public Field[] Fields;
	}

	sealed class Index: IndexKeyProvider
	{

		public class Field
		{
			public string Name;
			public IndexKeyProvider KeyProvider;

			public Ston.StonVariant GetValue(Ston.StonVariant root)
			{
				return root [Name];
			}
		}

		public class Row
		{
			public long RowId;
			public Ston.StonVariant Value;
		}

		sealed class FieldItem
		{
			public Field Field;
			public int Offset;
			public int KeyLength;
			public IKeyComparer Comparer;
		}

		readonly FieldItem[] fields;
		readonly int length;
		readonly int rowIdOffset;
		IndexKeyComparer comparer;

		public Index (IEnumerable<Field> fields)
		{
			this.fields = 
				(from field in fields
				select new FieldItem () { 
					Field = field, 
					KeyLength = field.KeyProvider.KeyLength 
				}).ToArray ();

			length = 0;
			foreach (var field in this.fields) {
				field.Offset = length;
				length += field.KeyLength;
			}
			rowIdOffset = length;
			length += 8;
		}

		#region implemented abstract members of IndexKeyProvider

		public override bool EncodeKey (object value, byte[] output, int offset)
		{
			var row = (Row)value;
			if (row == null) {
				throw new ArgumentNullException ("value");
			}

			try {
				var root = row.Value;
				foreach (var field in this.fields) {
					var fieldValue = field.Field.GetValue(root);

					if (!field.Field.KeyProvider.EncodeKey(fieldValue, output, offset + field.Offset)) {
						return false;
					}
				}

				var bcvt = new InternalUtils.BitConverter(output);
				bcvt.Set(offset + rowIdOffset, row.RowId);
				return true;
			} catch (Ston.StonVariantException) {
				return false;
			}
		}

		public override bool SupportsType (Type type)
		{
			return typeof(Row).IsAssignableFrom(type);
		}

		public override IKeyComparer KeyComparer {
			get {
				if (comparer == null) {
					comparer = new IndexKeyComparer (this);
				}
				return comparer;
			}
		}

		public override object Parameters {
			get {
				return new IndexParameters () {
					Fields = (from field in fields
						select new IndexParameters.Field() {
							Name = field.Field.Name,
							Parameters = field.Field.KeyProvider.Parameters
						}).ToArray()
				};
			}
		}

		public override int KeyLength {
			get {
				return length;
			}
		}

		#endregion

		public long GetRowId(byte[] key, int offset)
		{
			var bcvt = new InternalUtils.BitConverter (key);
			return bcvt.GetInt64 (offset + rowIdOffset);
		}

		sealed class IndexKeyComparer: IKeyComparer
		{
			readonly Index index;

			public IndexKeyComparer(Index index)
			{
				this.index = index;
			}

			public int Compare (byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2)
			{
				if (length1 != index.KeyLength ||
					length2 != index.KeyLength) {
					throw new InvalidOperationException ("Invalid key length.");
				}

				foreach (var field in index.fields) {
					if (field.Comparer == null) {
						field.Comparer = field.Field.KeyProvider.KeyComparer;
					}
					var ret = field.Comparer.Compare (buffer1, offset1 + field.Offset, field.KeyLength,
						          buffer2, offset2 + field.Offset, field.KeyLength);
					if (ret != 0) {
						return ret;
					}
				}

				// Row ID
				var rowId1 = index.GetRowId (buffer1, offset1);
				var rowId2 = index.GetRowId (buffer2, offset2);
				return rowId1.CompareTo (rowId2);
			}

			public bool IsValidKey (byte[] key)
			{
				return true;
			}
				
			public bool Equals (byte[] x, byte[] y)
			{
				return Compare (x, 0, x.Length, y, 0, y.Length) == 0;
			}

			public int GetHashCode (byte[] obj)
			{
				throw new NotImplementedException ();
			}

		}
	}
}


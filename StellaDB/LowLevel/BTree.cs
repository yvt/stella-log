
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Yavit.StellaDB.LowLevel
{
	public class BTreeParameters
	{
		/// <summary>
		/// The maximum allowed length of the key. THis value is used to allocate a room for key.
		/// Unused portion might be used to store value in the tree directly.
		/// </summary>
		int maximumKeyLength = 96;

		public int MaximumKeyLength {
			get {
				return maximumKeyLength;
			}
			set {
				maximumKeyLength = value;
				if (value < 1) {
					throw new ArgumentOutOfRangeException ("value");
				}
			}
		}

	}

	public partial class BTree: IOrderedKeyValueStore
	{
		/*
		 *   B-Tree Design
		 * 
		 *  Order = N, Keys / Node = N - 1
		 *  Node Pointer Size = 8
		 *  Key Size = K
		 *  Item Pointer Size = 2
		 *  Page Size = P
		 *  Overflow Pointer Size = 8
		 *  Node Header = H
		 * 
		 * 
		 *    Internal Nodes
		 * 
		 *  First Item Size = 0
		 *  Item Size = IPS + KS + NPS = 10 + K
		 *  (10 + K) * (N - 1) <= P - H
		 *  N <= Floor [ (P - H) / (10 + K) ] + 1 ... (1)
		 * 
		 *  If we let N = 3, then we get
		 *   (10 + K) * 2 <= P - H
		 *   K <= Floor [(P - H) / 2] - 10 ... (2)
		 * 
		 *  After computing N, we can maximize K like this
		 *   K <= (P - H) / (N - 1) - 10
		 * 
		 * 
		 * 
		 *    Header (16 bytes)
		 * 
		 *  0  ~ 1   Header Magic
		 *  2  ~ 3   Flags
		 *           [ 0] Node is a leaf
		 *  4  ~ 5   Pointer to First Item
		 *  6  ~ 13  Pointer to First Child Node (non-leaf node only)
		 *  14 ~ 15  Reserved (Zero)
		 * 
		 *    Entries for Each Item
		 *  
		 *  Size       = K + 10
		 *  Base Index = H + Size * Index
		 *             = 16 + (K + IPS + NPS) * Index
		 *             = 16 + (K + 10) * Index
		 *  
		 *  0   ~ 1      Index for the Next Item or 0xffff
		 *  2   ~ 9      Child Node Pointer or Zero
		 *  10  ~ K + 9  Key
		 * 
		 *    Key with Embedded Value
		 *  
		 *  Key Length Bytes = Ceil [Log2 [Effective Key Size + 1] / 8]
		 *  K = Effective Key Size + (Key Length Bytes * 2) + Overflow Pointer Size
		 *    = EKS + Ceil [Log2 [EKS + 1] / 8] * 2 + 8
		 *  Maximum In-tree Value Length = K - (KLB + AKL + KLB)
		 *                               = 8 + (EKS - AKL)
		 *  
		 *  0   - KLB       ~ 1                          Actual Key Length
		 *  KLB             ~ KLB + AKL - 1              Key (AKL = Actual Key Length)
		 *  KLB + AKL       ~ KLB + AKL + KLB - 1        In-tree Value Length
		 *  if IVL > 0 then:
		 *  KLB + AKL + KLB ~ KLB + AKL + KLB + IVL - 1  In-tree Value
		 *  if IVL = 0 then:
		 *  KLB + AKL + KLB ~ KLB + AKL + KLB + 7        Overflow Page Pointer
		 * 
		 */

		internal const uint HeaderMagic = 0x38a9af10;
		internal const uint CurrentVersion = 0x00010000;

		readonly LowLevelDatabase db;
		readonly StellaDB.IO.Pager pager;
		long headerBlockId; // might be zeroed after dropped
		readonly IKeyComparer comparer;


		readonly HeaderBlock header;

		// number of bytes used to represent actual key length.
		readonly int keyLengthSize;

		// number of bytes used to represent the length of key and its contents.
		readonly int keyLength;

		// maximum number of children that one internal node can hold.
		readonly int order;

		// number of bytes used for each item in a node.
		// item contains key, next node pointer, and pointer to the child node (or zero).
		readonly int itemSize;

		// number of items every node (except root) should have at least.
		readonly int minimumNumItems;

		// used during binary search.
		readonly int[] indices;

		// number which is updated whenever tree is updated.
		int treeStateNumber = 0;

		NodeCursor cursor;

		internal BTree (LowLevelDatabase db, long blockId = -1, BTreeParameters param = null)
		{
			if (db == null) {
				throw new ArgumentNullException ("db");
			}
			this.db = db;
			pager = db.Pager;

			comparer = new DefaultKeyComparer ();

			this.headerBlockId = blockId;
			header = new HeaderBlock (this);

			if (blockId == -1) {
				if (param == null) {
					throw new ArgumentNullException ("param");
				}

				this.headerBlockId = db.Freemap.AllocateBlock ();

				try {
					header.Initialize ();

					// TODO: maximize key length
					header.MaximumEffectiveKeyLength = param.MaximumKeyLength;
				} catch {
					db.Freemap.DeallocateBlock (headerBlockId);
					throw;
				}
			} else {
				header.Load ();
			}

			// compute key length bytes and key length
			keyLengthSize = ComputeKeyLengthSizeForEffectiveKeyLength(header.MaximumEffectiveKeyLength);
			keyLength = header.MaximumEffectiveKeyLength + keyLengthSize * 2 + 8;
			itemSize = keyLength + 10;

			// compute order of B-tree
			order = (pager.BlockSize - NodeBlock.NodeHeaderSize) / itemSize;
			if (order < 3) {
				throw new InvalidOperationException ("Order cannot be less than 3. " +
					"This error is usually caused by too small block size or too large key size.");
			}
			indices = new int[order - 1];

			minimumNumItems = (order - 1) / 2;

			if (header.RootNodeBlockId != 0) {
				var r = new NodeBlock (this);
				r.LoadRoot (header.RootNodeBlockId);
				cursor = new NodeCursor (r);
			}
		}

		public LowLevelDatabase Database
		{
			get { 
				CheckNotDropped ();
				return db;
			}
		}

		public long BlockId
		{
			get { 
				CheckNotDropped ();
				return headerBlockId;
			}
		}

		public long UserInfo1
		{
			get { return header.UserInfo1; }
			set { header.UserInfo1 = value; }
		}
		public long UserInfo2
		{
			get { return header.UserInfo2; }
			set { header.UserInfo2 = value; }
		}

		public int MaximumKeyLength
		{
			get {
				return header.MaximumEffectiveKeyLength;
			}
		}

		public IKeyComparer KeyComparer
		{
			get {
				return comparer;
			}
		}

		int ComputeKeyLengthSizeForEffectiveKeyLength(int keyLengthVal)
		{
			return (InternalUtils.GetBitWidth (keyLengthVal) + 7) / 8;
		}

		void TreeStateAboutToBeUpdated()
		{
			++treeStateNumber;

			FlushAllEntriesForStructureModification ();
			DisactivateAllEnumerator ();
		}

		public void Dump(TextWriter w)
		{
			CheckNotDropped ();

			if (cursor == null) {
				w.WriteLine ("empty");
			} else {
				cursor.Nodes [0].Dump (w, 0);
			}
		}

		public string DumpToString()
		{
			var tw = new StringWriter ();
			Dump (tw);
			return tw.ToString ();
		}


	}
}


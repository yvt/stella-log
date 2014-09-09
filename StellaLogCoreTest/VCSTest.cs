using NUnit.Framework;
using System;
using System.Linq;

namespace Yavit.StellaLog.Core.Test
{
	// This test fixture uses ConfigManager to test VersionController.
	[TestFixture ()]
	public class VCSTest
	{
		[Test, ExpectedException(typeof(InvalidOperationException))]
		public void CommitFail ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					book.VersionController.CommitLocalModifications ("hehehe");
				}
			}
		}
		[Test]
		public void Commit ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
		[Test]
		public void Revert ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.RevertLocalModifications ();

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.Not.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
		[Test]
		public void CommitAndCommitNewRow ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					var nextRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test2"] = "Piyo";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (nextRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.That (book.Config ["Test2"], Is.EqualTo ("Piyo"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
		[Test]
		public void CommitAndCommitUpdateRow ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					var nextRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Piyo";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (nextRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Piyo"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}

		[Test]
		public void CommitAndRevertNewRow ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					var nextRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test2"] = "Piyo";
					book.VersionController.RevertLocalModifications ();

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.EqualTo (nextRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.That (book.Config ["Test2"], Is.Not.EqualTo ("Piyo"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
		[Test]
		public void CommitAndRevertUpdateRow ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					var nextRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Piyo";
					book.VersionController.RevertLocalModifications ();

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.EqualTo (nextRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}

		[Test, ExpectedException(typeof(InvalidOperationException))]
		public void CheckoutNonexistent ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					byte[] hoge = book.VersionController.GenerateRevisionId ();
					book.VersionController.CheckoutRevision (hoge);
				}
			}
		}
		[Test]
		public void CommitAndCheckoutOriginal ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					book.VersionController.CheckoutRevision (initialRevision);

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.Not.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
		[Test]
		public void CommitAndMoveBranchToOriginal ()
		{
			using (var tmp = new TemporaryLogBook()) {
				using (var book = tmp.Open()) {
					var initialRevision = book.VersionController.GetCurrentRevision ();

					book.Config["Test"] = "Hoge";
					book.VersionController.CommitLocalModifications ("Test");

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.Not.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);

					book.VersionController.SetCurrentRevision (initialRevision);

					Assert.That (book.VersionController.GetCurrentRevision (),
						Is.EqualTo (initialRevision));
					Assert.That (book.Config ["Test"], Is.Not.EqualTo ("Hoge"));
					Assert.IsFalse (book.VersionController.HasLocalModifications());
					Assert.IsFalse (book.VersionController.HasUnresolvedMerge);
				}
			}
		}
	}
}


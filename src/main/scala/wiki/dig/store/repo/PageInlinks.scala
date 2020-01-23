package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


case class PageInlink(id: Int,
                      inLink: Int)

object PageInlinkRepo extends Repo[PageInlink] {

  import profile.api._

  class PageInlinkTable(tag: Tag) extends
    Table[PageInlink](tag, "page_inlinks") {

    def id = column[Int]("id", O.PrimaryKey)

    def inLink = column[Int]("inLinks")

    def * = (id, inLink) <> (PageInlink.tupled, PageInlink.unapply)
  }

  val entities = TableQuery[PageInlinkTable]

  def findInlinksById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.inLink).result
  }
}
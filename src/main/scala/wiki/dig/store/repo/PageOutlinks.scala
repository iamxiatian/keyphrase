package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


case class PageOutlink(id: Int,
                       outLink: Int)

object PageOutlinkRepo extends Repo[PageOutlink] {

  import profile.api._

  class PageOutlinkTable(tag: Tag) extends
    Table[PageOutlink](tag, "page_outlinks") {

    def id = column[Int]("id", O.PrimaryKey)

    def outLink = column[Int]("outLinks")

    def * = (id, outLink) <> (PageOutlink.tupled, PageOutlink.unapply)
  }

  val entities = TableQuery[PageOutlinkTable]

  def findOutlinksById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.outLink).result
  }
}
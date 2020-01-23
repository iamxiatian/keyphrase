package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


case class CategoryOutlink(id: Int,
                           outLink: Int)

object CategoryOutlinkRepo extends Repo[CategoryOutlink] {

  import profile.api._

  class CategoryOutlinkTable(tag: Tag) extends
    Table[CategoryOutlink](tag, "category_outlinks") {

    def id = column[Int]("id", O.PrimaryKey)

    def outLink = column[Int]("outLinks")

    def * = (id, outLink) <> (CategoryOutlink.tupled, CategoryOutlink.unapply)
  }

  val entities = TableQuery[CategoryOutlinkTable]

  def findOutlinksById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.outLink).result
  }
}
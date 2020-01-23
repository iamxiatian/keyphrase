package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


case class CategoryInlink(id: Int,
                          inLink: Int)

object CategoryInlinkRepo extends Repo[CategoryInlink] {

  import profile.api._

  class CategoryInlinkTable(tag: Tag) extends
    Table[CategoryInlink](tag, "category_inlinks") {

    def id = column[Int]("id", O.PrimaryKey)

    def inLink = column[Int]("inLinks")

    def * = (id, inLink) <> (CategoryInlink.tupled, CategoryInlink.unapply)
  }

  val entities = TableQuery[CategoryInlinkTable]

  def findInlinksById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.inLink).result
  }
}
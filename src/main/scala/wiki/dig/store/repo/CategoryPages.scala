package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


/**
  * 分类ID到拥有的维基文章(Page)之间的关系。注意： category_pages的主键是类别的id，
  * pages是Page，但表page_categories中，id是page的id，pages字段则是category的id
  *
  * @param id
  * @param pageId
  */
case class CategoryPage(id: Int,
                        pageId: Int)

object CategoryPageRepo extends Repo[CategoryPage] {

  import profile.api._

  class CategoryPagesTable(tag: Tag) extends
    Table[CategoryPage](tag, "category_pages") {

    def id = column[Int]("id", O.PrimaryKey)

    def pageId = column[Int]("pages")

    def * = (id, pageId) <> (CategoryPage.tupled, CategoryPage.unapply)
  }

  val entities = TableQuery[CategoryPagesTable]

  def findPagesById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.pageId).result
  }
}
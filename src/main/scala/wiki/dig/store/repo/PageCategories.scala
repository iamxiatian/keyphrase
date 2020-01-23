package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


/**
  * 维基文章(Page)到隶属的分类ID之间的关系。注意： page_categories的主键是页面的id，
  * pages是Category
  *
  * @param id
  * @param pageId
  */
case class PageCategory(id: Int,
                        pageId: Int)

object PageCategoryRepo extends Repo[PageCategory] {

  import profile.api._

  class PageCategoryTable(tag: Tag) extends
    Table[PageCategory](tag, "page_categories") {

    def id = column[Int]("id", O.PrimaryKey)

    def pageId = column[Int]("pages")

    def * = (id, pageId) <> (PageCategory.tupled, PageCategory.unapply)
  }

  val entities = TableQuery[PageCategoryTable]

  def findCategoriesById(id: Int): Future[Seq[Int]] = db.run {
    entities.filter(_.id === id).map(_.pageId).result
  }
}
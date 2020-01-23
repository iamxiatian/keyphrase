package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


case class Page(id: Int,
                pageId: Int,
                name: String,
                text: String,
                isDisambiguation: Boolean)

object PageRepo extends Repo[Page] {

  import profile.api._

  class PageTable(tag: Tag) extends
    Table[Page](tag, "Page") {

    def id = column[Int]("id", O.PrimaryKey)

    def pageId = column[Int]("pageId")

    def name = column[String]("name")

    def text = column[String]("text")

    def isDisambiguation = column[Boolean]("isDisambiguation")

    def * = (id, pageId, name, text, isDisambiguation) <> (Page.tupled, Page.unapply)
  }

  val pages = TableQuery[PageTable]

  def findByPageId(pageId: Int): Future[Option[Page]] = db.run {
    pages.filter(_.pageId === pageId).result.headOption
  }

  def findByName(name: String): Future[Option[Page]] = db.run {
    pages.filter(_.name === name).result.headOption
  }

  def count(): Future[Int] = db run {
    pages.length.result
  }

  def maxId(): Future[Option[Int]] = db run {
    pages.map(_.id).max.result
  }

  /**
    * 获取维基页面的基本信息，不包含全文
    *
    * @param limit
    * @return
    */
  def listBasicInfoFromId(fromId: Int,
                          limit: Int): Future[Seq[(Int, String, Boolean)]] =
    db run {
      pages.filter(_.id >= fromId)
        .sortBy(_.id)
        .map(r => (r.id, r.name, r.isDisambiguation))
        .take(limit)
        .result
    }

  def listFromId(fromId: Int,
                 limit: Int): Future[Seq[Page]] = db run {
    pages.filter(_.id >= fromId)
      .sortBy(_.id)
      .take(limit)
      .result
  }
}
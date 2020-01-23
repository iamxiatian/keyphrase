package wiki.dig.store.repo

import wiki.dig.store.repo.core.Repo

import scala.concurrent.Future


/**
  * 表示redirects指向的id
  */
case class PageRedirect(id: Int, redirects: String)

object PageRedirectRepo extends Repo[PageRedirect] {

  import profile.api._

  class PageRedirectTable(tag: Tag) extends
    Table[PageRedirect](tag, "page_redirects") {

    def id = column[Int]("id", O.PrimaryKey)

    def redirects = column[String]("redirects")

    def * = (id, redirects) <> (PageRedirect.tupled, PageRedirect.unapply)
  }

  val entities = TableQuery[PageRedirectTable]

  def findRedirects(pageId: Int): Future[Seq[String]] = db.run {
    entities.filter(_.id === pageId).map(_.redirects).result
  }
}
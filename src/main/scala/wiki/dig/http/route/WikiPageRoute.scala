package wiki.dig.http.route

import io.circe.syntax._
import spark.Spark._
import spark.{Request, Response, Route}
import wiki.dig.store.db.{PageContentDb, PageDb}
import wiki.dig.util.Logging

object WikiPageRoute extends JsonSupport with Logging {
  def register(): Unit = {
    //获取账号根据邮箱
    get("/wiki/page", "text/html", showPageHtml)
    get("/wiki/page_content", "text/html", showPageContent)
  }

  private def showPageContent: Route = (request: Request, _: Response) => {
    Option(request.queryMap("id").value()).flatMap(_.toIntOption) match {
      case Some(id) =>
        PageContentDb.getContent(id).getOrElse("")
      case None =>
        ""
    }
  }

  /**
    * 把一组由维基词条id构成的序列，转换为一组链接拼成的字符串，方便在网页中展示
    *
    * @param links
    * @return
    */
  private def linkToHtml(links: Seq[Int]): String = {
    links.flatMap {
      id =>
        PageDb.getNameById(id).map {
          linkName =>
            val count = PageDb.getLinkedCount(id)
            (linkName, count)
        }
    }
      .sortBy(_._2)(Ordering.Int.reverse)
      .map {
        case (linkName, count) =>
          val linkId = PageDb.getIdByName(linkName).getOrElse(0)
          s"""<li><a href="?name=${linkName}">${linkName}</a>($count)<a href="/wiki/page_content?id=${linkId}">详情</a></li>"""
      }
      .mkString("<ul>", "\n", "</ul>")
  }

  def showPageHtml: Route = (request: Request, _: Response) => {
    //val name = Option(request.params(":name")).getOrElse("").trim
    val name = Option(request.queryMap("name").value()).getOrElse("").trim
    PageDb.getIdByName(name) match {
      case Some(pageId) =>
        val inlinks = linkToHtml(PageDb.getInlinks(pageId))
        val outlinks = linkToHtml(PageDb.getOutlinks(pageId))

        s"""
           |<html>
           |<head><title>${name}</title></head>
           |<body>
           |  <h1>$name</h1>
           |  <h2>Inlinks:</h2>
           |  ${inlinks}
           |  <h2>Outlinks:</h2>
           |  ${outlinks}
           |</body>
           |""".stripMargin
      case None =>
        "该词条不存在"
    }
  }

  def showPageJson: Route = (request: Request, _: Response) => {
    val name = Option(request.params(":name")).getOrElse("").trim
    PageDb.getIdByName(name) match {
      case Some(pageId) =>
        val inlinks = PageDb.getInlinks(pageId).flatMap {
          id =>
            PageDb.getNameById(id).map(linkName => (id.asJson, linkName.asJson))
        }.asJson

        val outlinks = PageDb.getInlinks(pageId).flatMap {
          id =>
            PageDb.getNameById(id).map(linkName => (id.asJson, linkName.asJson))
        }.asJson

        jsonOk(
          Map("id" -> pageId.asJson,
            "name" -> name.asJson,
            "inlinks" -> inlinks.asJson,
            "outlinks" -> outlinks.asJson
          ).asJson
        )
      case None =>
        jsonError(s"词条不存在：$name")
    }
  }
}

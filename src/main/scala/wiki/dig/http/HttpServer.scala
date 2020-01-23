package wiki.dig.http

import better.files.File
import spark.Spark.{port, staticFiles, _}
import spark.{Request, Response}
import wiki.dig.MyConf
import wiki.dig.http.route.{JsonSupport, KeywordRoute, PaperRoute, WikiPageRoute}
import wiki.dig.util.{Logging, Segment}

/**
  * 启动服务的入口程序
  */
object HttpServer extends App with Logging {
  Segment.init()

  port(MyConf.webPort)
  staticFiles.externalLocation(File(MyConf.webRoot).canonicalPath)

  println(s"web root: ${File(MyConf.webRoot).canonicalPath}")

  // 注册路由
  WikiPageRoute.register()
  KeywordRoute.register()
  PaperRoute.register()

  before((request, response) => {
    def foo(request: Request, response: Response): Unit = new JsonSupport {
      LOG.debug(request.requestMethod() + ": " + request.pathInfo())

      val accessable = true
      if (!accessable) halt(401, jsonError("非授权访问！"))
    }

    foo(request, response)
  })

  //配置日志
  Logging.configure()

  LOG.info(s"start at http://localhost:${MyConf.webPort}")
}


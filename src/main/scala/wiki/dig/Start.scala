package wiki.dig

import java.io.File

import wiki.dig.common.BuildInfo
import wiki.dig.expt.{EmbeddingDb, ExptDb}
import wiki.dig.store.db.{CategoryDb, CategoryHierarchyDb, PageContentDb, PageDb}

/**
  * Application Start
  *
  * 构建维基百科的全文本地数据库：
  * nohup ./bin/start --buildPageDb --buildPageContentDb --startId=0 --batchSize=1000 &
  */
object Start extends App {

  case class Config(
                     buildCategoryPairs: Boolean = false,
                     buildHierarchy: Boolean = false,
                     buildPageDb: Boolean = false,
                     buildPageContentDb: Boolean = false,
                     buildEmbedding: Boolean = false,
                     outPageEmbedding: Boolean = false,
                     sample: Option[Int] = None,
                     startId: Int = 0,
                     batchSize: Int = 1000,
                     inFile: Option[File] = None
                   )

  val parser = new scopt.OptionParser[Config]("bin/spider") {
    head(s"${BuildInfo.name}", s"${BuildInfo.version}")

    import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

    val format: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

    opt[Unit]("buildCategoryPairs").action((_, c) =>
      c.copy(buildCategoryPairs = true)).text("build category info to rocksdb.")

    opt[Unit]("buildHierarchy").action((_, c) =>
      c.copy(buildHierarchy = true)).text("build category Hierarchy to rocksdb.")

    opt[Unit]("buildPageDb").action((_, c) =>
      c.copy(buildPageDb = true)).text("build page db with rocksdb format.")

    opt[Unit]("buildPageContentDb").action((_, c) =>
      c.copy(buildPageContentDb = true)).text("build page content db with rocksdb format.")

    opt[Unit]("buildEmbedding").action((_, c) =>
      c.copy(buildEmbedding = true)).text("build Embedding(must specify param f).")

    opt[Unit]("outPageEmbedding").action((_, c) =>
      c.copy(outPageEmbedding = true)).text("读取sample.page.ids.txt中的文章，将对应的embedding输出到sample.page.embedding.txt.")

    opt[String]('i', "inFile").optional().action((x, c) =>
      c.copy(inFile = Option(new File(x)))).text("input file name")

    opt[Int]('s', "sample").optional().
      action((x, c) => c.copy(sample = Some(x))).
      text("sample n triangles.")

    opt[Int]("startId").action(
      (x, c) =>
        c.copy(startId = x)
    ).text("build db from specified page id.")

    opt[Int]("batchSize").action(
      (x, c) =>
        c.copy(batchSize = x)
    ).text("batch size when build db.")

    help("help").text("prints this usage text")

    note("\n xiatian, xia(at)ruc.edu.cn.")
  }

  println(MyConf.screenConfigText)

  parser.parse(args, Config()) match {
    case Some(config) =>
      if (config.buildCategoryPairs) {
        CategoryDb.build()
        //CategoryDb.close()
      }

      if (config.buildHierarchy) {
        CategoryHierarchyDb.build()
        CategoryHierarchyDb.calculateArticleCount()
        //CategoryHierarchyDb.close()
      }

      if (config.sample.nonEmpty) {
        val n = config.sample.get
        CategoryHierarchyDb.sample(n)
      }

      if (config.buildPageDb) {
        println("build page db ...")
        PageDb.build(config.startId, config.batchSize)
        //PageDb.close()
      }

      if (config.buildPageContentDb) {
        println("build page db ...")
        PageContentDb.build(config.startId, config.batchSize)
        // PageContentDb.close()
      }

      if (config.buildEmbedding) {
        if (config.inFile.isEmpty) {
          println("input file not specified.")
        } else {
          EmbeddingDb.build(config.inFile.get)
        }
      }

      if (config.outPageEmbedding) {
        ExptDb.buildArticleEmbedding()
      }
    case None => {
      println("""Wrong parameters :(""".stripMargin)
    }

  }

  CategoryDb.close()
  CategoryHierarchyDb.close()
  PageDb.close()
  PageContentDb.close()
}

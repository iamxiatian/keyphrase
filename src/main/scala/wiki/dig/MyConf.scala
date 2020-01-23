package wiki.dig

import java.io.File

import com.typesafe.config.impl.Parseable
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import wiki.dig.common.BuildInfo

/**
  * System Settings configuration
  *
  * @author Tian Xia
  *         June 04, 2017 13:20
  */
object MyConf {
  val version = BuildInfo.version

  /** AkkaSystem使用的配置类，其启动时需要指定该类 */
  var akkaMasterConfig: Option[Config] = None

  //先采用my.conf中的配置，再使用application.conf中的默认配置
  lazy val config: Config = {
    val confFile = new File("./conf/my.conf")

    //先采用conf/my.conf中的配置，再使用application.conf中的默认配置
    if (confFile.exists()) {
      println(s"启用配置文件${confFile.getCanonicalPath}")
      val unresolvedResources = Parseable
        .newResources("application.conf", ConfigParseOptions.defaults())
        .parse()
        .toConfig()

      ConfigFactory.parseFile(confFile).withFallback(unresolvedResources).resolve()
    } else {
      ConfigFactory.load()
    }
  }

  def getString(path: String) = config.getString(path)

  def getInt(path: String) = config.getInt(path)

  def getBoolean(path: String) = config.getBoolean(path)

  def wikiDbUrl = config.getString("wiki.db.mysql.url")

  def wikiDbUser = config.getString("wiki.db.mysql.user")

  def wikiDbPassword = config.getString("wiki.db.mysql.password")

  lazy val dbRootDir = new File(config.getString("dig.db.root.dir"))

  lazy val wikiLang = config.getString("dig.wiki.lang")

  lazy val categoryDbReadOnly = config.getBoolean("dig.db.category.readonly")
  lazy val pageDbReadOnly = config.getBoolean("dig.db.page.readonly")

  val webPort = config.getInt("dig.http.port")
  val webRoot = config.getString("dig.http.root")
  val webVerify = config.getBoolean("dig.http.verify")

  val screenConfigText: String = {
    s"""
       |My configuration(build: ${BuildInfo.builtAtString}):
       |├── dig db:
       |│   ├── root path ==> ${dbRootDir.getCanonicalPath}
       |│   ├── category db readonly ==> $categoryDbReadOnly
       |│   ├── page db readonly ==> $pageDbReadOnly
       |│   └── API http port ==> ssss
       |│
       |├── mysql config:
       |│   ├── url ==> $wikiDbUrl
       |│   ├── user ==> $wikiDbUser
       |│   └── password ==> $wikiDbPassword
       |│
       |└── wiki config:
       |    ├── language ==> $wikiLang
       |    └── API http port ==> ssss
       |"""
  }

}


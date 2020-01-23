package wiki.dig.store.repo.core

import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * 处理数据的Repository, 目前支持MysqlRepo和PostgresRepo两类.
  *
  * @tparam T
  */
trait Repo[T] {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val profile = MySQLProfile

  val db = RepoProvider.mysqlDb

  val LOG = LoggerFactory.getLogger(this.getClass)
}

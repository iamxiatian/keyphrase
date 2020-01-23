package wiki.dig.store.repo

import java.util.concurrent.LinkedBlockingQueue

import wiki.dig.MyConf
import wiki.dig.store.repo.core.Repo

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
  * 经过观察数据库，发现id和pageId对于Category数据是一样的。即二者完全相同
  *
  * @param id
  * @param pageId
  * @param name
  */
case class Category(id: Int,
                    pageId: Int,
                    name: String)

object CategoryRepo extends Repo[Category] {

  import profile.api._

  class CategoryTable(tag: Tag) extends
    Table[Category](tag, "Category") {

    def id = column[Int]("id", O.PrimaryKey)

    def pageId = column[Int]("pageId")

    def name = column[String]("name")

    def * = (id, pageId, name) <> (Category.tupled, Category.unapply)
  }

  val entities = TableQuery[CategoryTable]

  def findById(id: Int): Future[Option[Category]] = db.run {
    entities.filter(_.id === id).result.headOption
  }

  def findByIds(ids: Seq[Int]): Future[Seq[Category]] = db.run {
    entities.filter(_.id.inSet(ids.toSet)).result
  }

  def list(page: Int, limit: Int): Future[Seq[Category]] = db run {
    entities.drop((page - 1) * limit).take(limit).result
  }

  def count(): Future[Int] = db run {
    entities.length.result
  }

  /**
    * 根分类，对于英文来说，根的名称为Contents
    *
    * @return
    */
  def root(): Future[Option[Category]] = db.run {
    if(MyConf.wikiLang == "zh")
      entities.filter(r => r.name === "页面分类" || r.name === "頁面分類").result.headOption
    else
      entities.filter(_.name === "Main_topic_classifications").result.headOption
  }

  /**
    * 获取第一级有效的分类
    *
    * @return
    */
  def levelOne(): Future[Seq[Category]] = root().flatMap {
    case Some(r) =>
      CategoryOutlinkRepo.findOutlinksById(r.id).flatMap {
        ids => findByIds(ids)
      }
    case None => Future.successful(Seq.empty[Category])
  }

  def process() = {
    val q = new LinkedBlockingQueue[Category]()

    val ones = Await.result(levelOne(), Duration.Inf)
    ones.foreach(q.add)

    while (!q.isEmpty) {
      val current = q.poll()
      //处理当前节点


    }
  }


}
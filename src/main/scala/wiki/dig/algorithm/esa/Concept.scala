package wiki.dig.algorithm.esa

/**
  *
  * @param idx 索引的顺序编号
  * @param id 概念的id，目前为维基网页的文章id
  * @param title 概念的标题，维基词条的标题
  * @param content  文本内容
  * @param alias 别名列表
  * @param inlinks 入链id
  * @param outlinks 出链id
  * @param categories 隶属的类别id
  */
case class Concept(idx: Int,
                   id: Int,
                   title: String,
                   content: String,
                   alias: Seq[Int],
                   inlinks: Seq[Int],
                   outlinks: Seq[Int],
                   categories: Seq[Int]
                  ) {

}

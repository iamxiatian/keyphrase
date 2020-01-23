package wiki.dig.algorithm

import java.io.File

import wiki.dig.parser.WikiPage
import wiki.dig.store.db.{CategoryHierarchyDb, PageContentDb, PageDb}

class EspmExpt {
  def firstRun(): Unit = {

    //@SEE PageDump
    //将层次类别中涉及的文章id输出到文件中
    val pageIdFile = new File("./dump/page_id.txt")
    val pageIds = CategoryHierarchyDb.dumpPageIds(pageIdFile)

    //输出pageIds对应的文章内容

    var idx = 0
    pageIds foreach {
      case pageId =>
        val title = PageDb.getNameById(pageId)
        val plainText = PageContentDb.getContent(pageId).map {
          t =>
            WikiPage.getPlainText(t)
        }


//        val c = new FullConcept
      //        c.setId(idx + 1)
      //        c.setTitle(page.getTitle)
      //        c.setPlainContent(page.getPlainText)
      //        c.setRawContent(page.getText)
      //        c.setOutLinks(page.getInternalLinks)
      //        c.setOutId(Integer.toString(page.getId))
      //        c.setCategories(page.getCategories)
      //        c.setAliasNames(page.getAliases)
    }
  }

  def build(): Unit = {


  }
}

package wiki.dig.parser

import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.{MediaWikiParser, MediaWikiParserFactory}

import scala.io.Source
import scala.jdk.CollectionConverters._

class WikiPage {

}

object WikiPage {
  /**
    * 对于维基百科带有格式的内容，清洗后仅保留纯文本
    *
    * @param wikiContent
    */
  def getPlainText(wikiContent: String): Option[String] = {
    val pf = new MediaWikiParserFactory()
    val parser: MediaWikiParser = pf.createParser
    val pp: ParsedPage = parser.parse(wikiContent)

    if (pp == null)
      None
    else Some(
      pp.getSections.asScala.map {
        section =>
          val sectionTitle = Option(section.getTitle)

          val sectionContent = section.getParagraphs.asScala.map(_.getText).filter {
            p =>
              !p.startsWith("TEMPLATE") && !p.matches("[^:]+:[^\\ ]+")
          }.mkString("\n")

          if (sectionContent.isEmpty)
            ""
          else if (sectionTitle.isEmpty)
            sectionContent
          else
            s"${sectionTitle.getOrElse("")}\n\n${sectionContent}"
      }.filter(_.nonEmpty).mkString("\n\n")
    )
  }

  def main(args: Array[String]): Unit = {
    val text = Source.fromFile("./samples/wiki-text3.txt").getLines().mkString("\n")
    println(getPlainText(text).getOrElse("<EMPTY>"))
  }
}

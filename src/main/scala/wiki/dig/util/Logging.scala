package wiki.dig.util

import java.io.{File, FileInputStream}

import org.slf4j.LoggerFactory

trait Logging {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def write(s: String) = print(s)

  /**
    * 输出到控制台
    */
  def writeLine(s: String) = println(s)

  def readLine(): String = scala.io.StdIn.readLine()

  def readLine(text: String, args: Any*): String = scala.io.StdIn.readLine(text, args)
}

object Logging {
  def println(msg: String) = {
    System.out.println(s"LOG: $msg")
  }

  /**
    * 配置日志，可以在程序启动的时候调用该方法
    */
  def configure(): Unit = {
    val f = new File("./conf/logback.xml")
    if (f.exists()) {
      import ch.qos.logback.classic.LoggerContext
      import ch.qos.logback.classic.joran.JoranConfigurator
      import org.slf4j.LoggerFactory
      val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
      loggerContext.reset()
      val configurator = new JoranConfigurator
      val configStream = new FileInputStream(f)
      configurator.setContext(loggerContext)
      configurator.doConfigure(configStream) // loads logback file
      configStream.close()
      println("finished to configure logback.")
    }
  }
}

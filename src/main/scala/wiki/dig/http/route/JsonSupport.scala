package wiki.dig.http.route

import io.circe._
import io.circe.parser._
import io.circe.syntax._

trait JsonSupport {
  val SUCCESS_CODE: Int = 0
  val ERROR_CODE: Int = -1

  def jsonOk(data: String): String = jsonOk(data.asJson)

  /**
    * 把Json字符串解析为JSON对象
    *
    * @param jsonString
    * @return
    */
  def parseString(jsonString: String): Json = parse(jsonString) match {
    case Right(json) => json
    case Left(e) => e.getMessage().asJson
  }

  def jsonOk(data: Json): String = Map(
    "code" -> SUCCESS_CODE.asJson,
    "data" -> data).asJson.printWith(Printer.spaces2)

  def jsonOk(data: Seq[Json]): String = Map(
    "code" -> SUCCESS_CODE.asJson,
    "data" -> data.asJson).asJson.printWith(Printer.spaces2)

  def jsonError(message: String): String = Map(
    "code" -> ERROR_CODE.asJson,
    "message" -> message.asJson).asJson.printWith(Printer.spaces2)

  def jsonError(message: String, data: Json): String = Map(
    "code" -> SUCCESS_CODE.asJson,
    "message" -> message.asJson,
    "data" -> data).asJson.printWith(Printer.spaces2)
}
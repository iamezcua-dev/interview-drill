//> using scala 2.13
//> using jvm 17
//> using javaOpt -XX:+IgnoreUnrecognizedVMOptions
//> using javaOpt --add-opens=java.base/java.lang=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.io=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.net=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.security.action=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
//> using javaOpt -Djdk.reflect.useDirectMethodHandle=false

// Grader harness for Scala solutions. Reads a JSON spec with every case,
// builds the input DataFrames, calls Solution.solution(...) with one
// DataFrame per table (in tables.yaml order) and writes the results.

import java.io.File
import java.lang.reflect.InvocationTargetException

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object DrillHarness {
  private val json = new ObjectMapper()
  private val nodes = JsonNodeFactory.instance

  private def cell(node: JsonNode, dataType: DataType): Any =
    if (node == null || node.isNull) null
    else dataType match {
      case IntegerType    => node.asInt()
      case LongType       => node.asLong()
      case DoubleType     => node.asDouble()
      case BooleanType    => node.asBoolean()
      case _: DecimalType => new java.math.BigDecimal(node.asText())
      case DateType       => java.sql.Date.valueOf(node.asText())
      case TimestampType  =>
        java.sql.Timestamp.valueOf(node.asText().replace('T', ' '))
      case _              => node.asText()
    }

  private def output(value: Any): JsonNode = value match {
    case null                    => nodes.nullNode()
    case v: java.math.BigDecimal => nodes.numberNode(v)
    case v: Int                  => nodes.numberNode(v)
    case v: Long                 => nodes.numberNode(v)
    case v: Double               => nodes.numberNode(v)
    case v: Float                => nodes.numberNode(v)
    case v: Short                => nodes.numberNode(v)
    case v: Byte                 => nodes.numberNode(v)
    case v: Boolean              => nodes.booleanNode(v)
    case v: java.sql.Date        => nodes.textNode(v.toLocalDate.toString)
    case v: java.sql.Timestamp   =>
      nodes.textNode(v.toLocalDateTime.toString.replace('T', ' '))
    case v                       => nodes.textNode(v.toString)
  }

  private def frame(spark: SparkSession, table: JsonNode): DataFrame = {
    val schema = StructType.fromDDL(table.get("ddl").asText())
    val rows = table.get("rows").elements().asScala.map { row =>
      Row.fromSeq(schema.fields.toIndexedSeq.zipWithIndex.map {
        case (field, i) => cell(row.get(i), field.dataType)
      })
    }.toList
    spark.createDataFrame(rows.asJava, schema)
  }

  private def rootCause(t: Throwable): Throwable = t match {
    case e: InvocationTargetException if e.getCause != null =>
      rootCause(e.getCause)
    case e: ExceptionInInitializerError if e.getCause != null =>
      rootCause(e.getCause)
    case e => e
  }

  def main(args: Array[String]): Unit = {
    val spec = json.readTree(new File(args(0)))
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("drill-scala")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val results: ArrayNode = nodes.arrayNode()
    val target = Class.forName("Solution$").getField("MODULE$").get(null)
    val tableCount = spec.get("tables").size()
    val method = target.getClass.getMethods
      .find(m => m.getName == "solution" && m.getParameterCount == tableCount)

    spec.get("cases").elements().asScala.foreach { c =>
      val result = nodes.objectNode()
      method match {
        case None =>
          result.put(
            "error",
            s"define def solution(...) taking $tableCount DataFrame(s)"
          )
        case Some(m) =>
          try {
            val inputs = c.get("tables").elements().asScala
              .map(t => frame(spark, t)).toSeq
            val df = m.invoke(target, inputs: _*).asInstanceOf[DataFrame]
            if (df == null) result.put("error", "solution returned null")
            else {
              val cols = result.putArray("columns")
              df.columns.foreach(cols.add)
              val rows = result.putArray("rows")
              df.collect().foreach { r =>
                val row = rows.addArray()
                r.toSeq.foreach(v => row.add(output(v)))
              }
            }
          } catch {
            case t: Throwable =>
              val cause = rootCause(t)
              val msg = Option(cause.getMessage).getOrElse("")
              val name = cause.getClass.getSimpleName
              result.put("error", if (cause.isInstanceOf[NotImplementedError])
                "solution is not implemented" else s"$name: $msg")
          }
      }
      results.add(result)
    }
    json.writeValue(new File(args(1)), results)
    spark.stop()
  }
}

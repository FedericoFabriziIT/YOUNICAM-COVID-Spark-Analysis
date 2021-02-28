import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._

/*
* DESCRIPTION
* This class builds a property graph that links presences of different
* students in the same room in the same day; edges have an integer property that stands for 
* how many minutes the two presences are overlapped in time
*   - if the outDate is missing, the two presences will be considered at risk
*   - presence nodes will contain any data needed for further analysis
*
* The resulting graph is saved as object file in two directories(vertices, edges) in the current
* path, to then be parsed by the GraphAnalizer class. Those directories must not exist before executing this class.
* */

object GraphBuilder extends App {

  def minutesRangeOverlap(s1: Long, e1: Long, s2: Long, e2: Long): Int = {

    if(e1 == 0 || e2 == 0){
      return Int.MaxValue
    }
    else if((s1 == s2) && (e1 == e2)){
      return ((e1 - s1)/60).toInt
    }
    else if((s1 == s2) && (e1 < e2)){
      return ((e1 - s1)/60).toInt
    }
    else if((s1 == s2) && (e1 > e2)){
      return ((e2 - s1)/60).toInt
    }
    else if((s1 > s2) && (e1 == e2)){
      return ((e1 - s1)/60).toInt
    }
    else if((s1 < s2) && (e1 == e2)){
      return ((e1 - s2)/60).toInt
    }
    else if((s1 < s2) && (e1 < e2) && (e1 > s2)){
      return ((e1 - s2)/60).toInt
    }
    else if((s1 > s2) && (e1 > e2) && (s1 < e2)){
      return ((e2 - s1)/60).toInt
    }
    else if((s1 < s2) && (e1 > e2)){
      return ((e2 - s2)/60).toInt
    }
    else if((s1 > s2) && (e1 < e2)){
      return ((e1 - s1)/60).toInt
    }
    // not overlapped
    0
  }

  print("Provide a master parameter for the SparkSession(Press \"Enter\" for \"local[*]\"): ")
  var sparkMaster: String = scala.io.StdIn.readLine()
  if(sparkMaster == ""){
    sparkMaster = "local[*]"
  }
  println(s"Spark Master: $sparkMaster")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark YoUnicam for COVID analysis - Graph Builder")
    .master(sparkMaster)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Presences
  print("Provide the presences file path(Press \"Enter\" for \"data\\presences.csv\"): ")
  var presencesPath: String = scala.io.StdIn.readLine()
  if(presencesPath == ""){
    presencesPath = "data\\presences.csv"
  }

  println("Creating presences dataframe...")
  val presencesDF = spark.read
    .option("header", value = true)
    .option("inferschema", value = true)
    .csv(presencesPath)
    .select("_id", "id", "posto", "aula", "polo", "sede", "inDate", "outDate")
    .distinct().withColumn("v_id", monotonically_increasing_id())
    .withColumn("date", to_date(col("inDate"), "MM/dd/yy HH:mm"))
    .withColumnRenamed("id", "studente")
    // presencesDF.show(truncate = false)

  presencesDF.createTempView("presences")
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  // Creating Vertices
  println("Creating vertices...")
  val vertices: RDD[(VertexId, List[Any])] = presencesDF.select(
    "v_id",
    "studente",
    "posto",
    "aula",
    "polo",
    "sede",
    "inDate",
    "outDate",
    "_id").rdd
    .map(Row => (Row.get(0).asInstanceOf[Long],
      List(Row.get(1), Row.get(2), Row.get(3), Row.get(4), Row.get(5), Row.get(6), Row.get(7), Row.get(8))))

  // Creating Edges
  println("Creating edges...")
  val edgesDF = spark.sql("SELECT p1.v_id, p1.inDate as s1, p1.outDate as e1, " +
    "p2.v_id, p2.inDate as s2, p2.outDate as e2 " +
    "FROM presences as p1, presences as p2 " +
    "WHERE p1.v_id != p2.v_id AND " +
    "p1.studente != p2.studente AND " +
    "p1.aula = p2.aula AND " +
    "p1.polo = p2.polo AND " +
    "p1.sede = p2.sede AND " +
    "p1.date = p2.date")
    .withColumn("s1", unix_timestamp(col("s1"), "MM/dd/yy HH:mm"))
    .withColumn("e1", unix_timestamp(col("e1"), "MM/dd/yy HH:mm"))
    .withColumn("s2", unix_timestamp(col("s2"), "MM/dd/yy HH:mm"))
    .withColumn("e2", unix_timestamp(col("e2"), "MM/dd/yy HH:mm"))
  // edgesDF.show()

  // [p1.v_id, p1.inDate, p1.outDate, p2.v_id, p2.inDate, p2.outDate]
  val edges: RDD[Edge[Int]] = edgesDF.rdd
    .map(Row => Edge(Row.get(0).asInstanceOf[Long],
      Row.get(3).asInstanceOf[Long],
      minutesRangeOverlap(
        Row(1).asInstanceOf[Long], Row(2).asInstanceOf[Long],
        Row(4).asInstanceOf[Long], Row(5).asInstanceOf[Long]
      )))

  // Creating the Graph(will create vertices and edges directories)
  println("Saving the graph...")
  val graph = Graph(vertices, edges)
  graph.vertices.saveAsObjectFile("vertices")
  graph.edges.saveAsObjectFile("edges")
  println("Directories \"vertices\" and \"edges\" have been created.")

}

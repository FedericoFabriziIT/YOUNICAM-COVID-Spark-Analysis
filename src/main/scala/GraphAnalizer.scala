import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/*
* DESCRIPTION
* This class executes analysis on the graph built with GraphBuilder, reading it from the vertices and edges
* directories. The analysis consists in examining the neighbor nodes of every node that stands for a presence
* of a student that tested positive: those nodes refer to other presences in the same classroom in the same day,
* and if they also fall into risk criteria, the students they refer to should receive an alert.
* It is possible to specify some parameters:
*    - student_id: the id of the student that tested positive
*    - minutes: determines over how many minutes a presence near the positive student should be considered at risk
*    - days: determines within how many days in the past presences should be considered for the analysis
*
* The result is contained in a text file in which every line contains info about:
*   - the student that should be warned
*   - the location and the date of the presence that was considered at risk
*   - the two ids referred to the two presences that were two adjacent vertices in the graph

* */

object GraphAnalizer extends App {

  def computeNeighborSeats(rowLen: Int, totalSeats: Int, seat: Int): List[Int] = { // max 8 neighbors
    if (totalSeats < seat || seat < 1) throw new IllegalArgumentException("Invalid argument for computeNeighbors")

    val numRowsTemp = totalSeats / rowLen // lower integer part
    val numRows = if (totalSeats % rowLen == 0) numRowsTemp else numRowsTemp + 1

    val row: Int = (seat-1) / rowLen
    val col: Int = (seat-1) % rowLen

    // println(s"rowLen: $rowLen numRows: $numRows seat: $seat row: $row col: $col")

    // Matrix
    val matrix = Array.ofDim[Int](rowLen,numRows)
    for (c <- 0 until numRows){
      for (r <- 0 until rowLen){
        val sN = (c * rowLen) + r + 1
        if (sN <= totalSeats){
          // print(sN + "\t")
          matrix(r)(c) = sN
        }
      }
      // println("|")
    }

//    for (r <- matrix(0).indices) {
//      for (c <- matrix.indices) {
//        if (matrix(c)(r) != 0) print(matrix(c)(r)+"\t")
//      }
//       println("|")
//    }

    // adjacent seats, max 8
    var neighbors = new ListBuffer[Int]()
    var temp = 0
    Try(temp = matrix(col-1)(row-1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col)(row-1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col+1)(row-1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col+1)(row)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col+1)(row+1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col)(row+1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col-1)(row+1)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }
    Try(temp = matrix(col-1)(row)) match {
      case Success(_) => if (temp != 0) neighbors += temp
      case Failure(_) =>
    }

    //println()
    //println(neighbors.toList)
    neighbors.toList
  }

  def checkPresencesSeats(triplet: EdgeTriplet[List[Any], Int],
                          student: String,
                          minutes: Int, days: Int, rooms: DataFrame): Any = {
    val format = new SimpleDateFormat("MM/dd/yy HH:mm")
    // current time in ms - ms in a day * number of days
    val daysLimit: Long = System.currentTimeMillis() - 86400000L*days.asInstanceOf[Long]

    if(triplet.attr < minutes){
      return null
    }
    // (studente, posto, aula, polo, sede, inDate, outDate, presence_id)
    else if(format.parse(triplet.srcAttr(5).toString).getTime < daysLimit){
      return null
    }

    // presence of the positive student
    val presenceP = {
      if(triplet.srcAttr.head.toString == student){
        triplet.srcAttr
      }
      else{
        triplet.dstAttr
      }
    }

    // presence of a student that should potentially be warned
    val presenceW = {
      if(triplet.srcAttr.head.toString != student){
        triplet.srcAttr
      }
      else{
        triplet.dstAttr
      }
    }

    // (studente, posto, aula, polo, sede, inDate, outDate, presence_id)
    val posto = presenceP(1).asInstanceOf[Int]
    val aula = presenceP(2)
    val polo = presenceP(3)
    val sede = presenceP(4)
    val data = presenceP(5)

    val roomQ = rooms.filter((rooms("aulaId") === aula) &&
      (rooms("poloDes") === polo) &&
      (rooms("Sede") === sede))
    if(roomQ.count() != 1){
      return null
    }

    val room = roomQ.first()

    // these parameters could both be extracted from the dataset in a real case scenario
    // [Sede, _id, aulaDes, aulaId, capienza, date, poloDes, scheduleType, schedules]
    val totalSeats = Integer.parseInt(room.get(4).toString)
    val nSeats = computeNeighborSeats(5, totalSeats, posto)

    if(nSeats.contains(presenceW(1).asInstanceOf[Int])){
      return "Studente: " + presenceW.head.toString +
        s", Aula: $aula, Polo: $polo, Sede: $sede, Data: $data - " +
        "Id Presenze: " + presenceP.last.toString + ", " + presenceW.last.toString
        //presenceP + ", " + presenceW
    }
    null
  }

  def computeStudentsToNotify(student_id: String, graph: Graph[List[Any], Int],
                              minutes: Int, days: Int, rooms: DataFrame): List[Any] = {
    println(s"Checking presences of student $student_id in the last $days days in the graph...")
    val triplets = graph.triplets.filter(t => (t.srcAttr.head.toString == student_id) ||
      (t.dstAttr.head.toString == student_id))

    println("Searching for presences at risk...")

    val studentsToWarn = new ListBuffer[Any]()
    triplets.collect().foreach(t => studentsToWarn += checkPresencesSeats(t, student_id, minutes, days, rooms))
    studentsToWarn.toList
  }

  print("Provide a master parameter for the SparkSession(Press \"Enter\" for \"local[*]\"): ")
  var sparkMaster: String = scala.io.StdIn.readLine()
  if(sparkMaster == ""){
    sparkMaster = "local[*]"
  }
  println(s"Spark Master: $sparkMaster")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark YoUnicam for COVID analysis - Graph Analizer")
    .master(sparkMaster)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Loading graph and rooms data
  println("Loading the graph...")
  // [v_id, (studente, posto, aula, polo, sede, inDate, outDate, presence_id)]
  val vertices: RDD[(VertexId, List[Any])] = spark.sparkContext.objectFile("vertices")
  val edges: RDD[Edge[Int]] = spark.sparkContext.objectFile("edges")
  val graph: Graph[List[Any], Int] = Graph(vertices, edges)
  // graph.triplets.coalesce(1, true).saveAsTextFile("presencesGraph")

  print("Provide the rooms file path(Press \"Enter\" for \"data\\rooms.json\"): ")
  var roomsPath: String = scala.io.StdIn.readLine()
  if(roomsPath == ""){
    roomsPath = "data\\rooms.json"
  }

  println("Loading the rooms...")
  val roomsDF = spark.read
    .option("inferschema", value = true)
    .option("multiline", value = true)
    .json(roomsPath)
  // roomsDF.show(truncate = false)

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  print("Provide the parameters for the analysis(student_id: String, minutes: Int, days: Int), separated by a whitespace: ")
  val parametersS: String = scala.io.StdIn.readLine()
  val parametersL = parametersS.split(" ").toList
  val student_id: String = parametersL.head
  val minutes: Int = Integer.parseInt(parametersL(1))
  val days: Int = Integer.parseInt(parametersL(2))

  // Analysis starts
  val outFName = "ExposedStudents.txt"
  val ret = computeStudentsToNotify(student_id, graph, minutes, days, roomsDF).filter(v => v != null).distinct
  val writer = new BufferedWriter(new FileWriter(s"$outFName"))
  writer.write("These students may have been exposed to SARS-CoV-2 infection and should be warned:\n")
  writer.write("(Every line refers to a single student and includes where the contact has happened.)\n")
  ret.foreach(x => writer.write(x.toString+'\n'))
  writer.close()
  println(s"File $outFName has been created.")

}

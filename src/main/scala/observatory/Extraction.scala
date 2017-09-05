package observatory

/**


        SourceShape
      Outlet[String]                   FlowShape[String, Station]            
 ╔══════════════════════╗                ╔══════════════════════╗                
 ║                      ║                ║                      ║                
 ║                      ║                ║                      ║                
 ║                      ╠──┐          ┌──╣                      ║         
 ║  Source              │  │────────▶|   │   linesToStations    ║ 
 ║     .fromIterator()  ╠──┘          └──╣                      ║ 
 ║                      ║                ║                      ║  
 ║                      ║                ║                      ║   
 ║                      ║                ║                      ║   
 ╚══════════════════════╝                ╚═════════╗   ╔════════╝   
                                                   ╚ | ╝                                                       
                                                     |
                                                     |
                                                     ↓                                                        
                                                   ╔   ╗ UniformFanOutShape[Station, Station]                                                      
                                         ╔═════════║   ║══════════╗                                                               
                                         ║                        ║                                               
                                         ║                        ║                                              
                                         ║   Partitioner.         ║                                              
                                         ║  stationToOutputPort   ║                                              
                                         ║                        ║
                                         ║                        ║                                              
                                         ║                        ║                                              
                                         ║                        ║
                                         ╚══════════╗   ╔═════════╝   
                                                    ╚ | ╝                                                       
            |─────────────────────────────────────────|──────────────────────────────────────────────────|
            |                                         |                                                  |
            |                                         |                                                  |      
            |                                         |                                                  |
          ╔ ↓ ╗ Inlet[Station]                      ╔ ↓ ╗ Inlet[Station]                               ╔ ↓ ╗ Inlet[Station]   
╔═════════║   ║══════════╗                ╔═════════║   ║══════════╗                         ╔═════════║   ║══════════╗                                                     
║                        ║                ║                        ║                         ║                        ║                         
║                        ║                ║                        ║                         ║                        ║                        
║     Sink.              ║                ║      Sink.             ║                         ║     Sink.              ║                        
║       Seq[Station]     ║                ║        Seq[Station]    ║                         ║       Seq[Station]     ║                        
║                        ║                ║                        ║                         ║                        ║   
║                        ║                ║                        ║                         ║                        ║                        
║                        ║                ║                        ║                         ║                        ║                        
║                        ║                ║                        ║                         ║                        ║   
╚════════════════════════╝                ╚════════════════════════╝                         ╚════════════════════════╝
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                            
                                                                                     
  */




import scala.io.Source._
import akka.stream._
import akka.stream.scaladsl._

import akka.stream.scaladsl.{Source, Sink}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import scala.concurrent._
import scala.util.{Failure, Success}
import java.time.LocalDate

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import scala.language.postfixOps


/**
  * 1st milestone: data extraction
  */
object Extraction {


  implicit val actorSystem = ActorSystem("Read-Dir")
  implicit val ec = actorSystem.dispatcher
  implicit val materializer = ActorMaterializer()


  case class Station(stnId: Option[Int], wbanId: Option[Int], lat: Double, lon: Double)


  trait TemperatureRecord

  case class TmpRecStnIdOnly(stnId: Int, month: Int, day: Int, temperatureC: Double) extends TemperatureRecord
  case class TmpRecWbanIdOnly(wbanId: Int, month: Int, day: Int, temperatureC: Double) extends TemperatureRecord
  case class TmpRecStnAndWbanId(stnId: Int, wban: Int, month: Int, day: Int, temperatureC: Double) extends TemperatureRecord
  case class TmpRecErroneous(msg: String) extends TemperatureRecord



  trait Triplet
  case class ValidTriplet(date: LocalDate, location: Location, temperatureCelcious: Double) extends Triplet
  case class InvalidTriplet(msg: String) extends Triplet



  def stringIteratorFromFile(file: String) : Iterator[String] = {
    println("\n" + file + "\n")

    try {
      val resource = getClass.getResourceAsStream(file)

      if(resource == null)
        println(s"resource = null!!! No resource with name: ${file} is found!!!")

      scala.io.Source.fromInputStream(resource, "utf-8").getLines()

      //scala.io.Source.fromInputStream(getClass.getResourceAsStream(file) ,"utf-8").getLines()
    }
    catch {
      case ex: NullPointerException => {
        println(s"NullPointerException for file: ${file}!!! Name = Null!")
        new Array[String](1).toIterator
      }
      case _: Throwable => {
        println("??????????")
        new Array[String](1).toIterator
      }
    }


  }


  def linesToStations: Flow[String, Station, NotUsed] = {

    Flow[String].map(_.split(",", 4)) //Array[String](4)
      .filter(xs => ((xs(2) != "") && (xs(3) != "")) //&& //Must have Both Lon & Lat!
      /*((xs(0) != "") || (xs(1) != ""))*/ ) //Optional!
      .map { xs =>

      println(s"${xs(0)}, ${xs(1)}, ${xs(2)}, ${xs(3)},")

      var stnId: Option[Int] = None
      if (xs(0) != "")
        stnId = Some(xs(0).toInt)

      var wbanId: Option[Int] = None
      if (xs(1) != "")
        wbanId = Some(xs(1).toInt)


      new Station(stnId, wbanId, xs(2).toDouble, xs(3).toDouble)
    }
  }


  def fahrenheitToCelcious(tmpFahrenheit: Double): Double = {
    (tmpFahrenheit - 32) * 5.0 / 9.0
  }


  def linesToTemperatures: Flow[String, TemperatureRecord, NotUsed] = {

    Flow[String].map(_.split(",", 5)) //Array[String](4)
      .map { xs =>

      println(xs(0) + ", " + xs(1) + ", " + xs(2) + ", " + xs(3) + ", " + xs(4))

      try{

        var stnId: Option[Int] = None
        if (xs(0) != "")
          stnId = Some(xs(0).toInt)

        var wbanId: Option[Int] = None
        if (xs(1) != "")
          wbanId = Some(xs(1).toInt)


        var month: Int = 0
        var day: Int = 0
        var tmpC: Double = 0.0


        month = xs(2).toInt
        day = xs(3).toInt

        if(xs(4) == "9999.9")
          throw new Exception()
        else
          tmpC = fahrenheitToCelcious(xs(4).toDouble)


        if((stnId isDefined) && (wbanId isEmpty))
          new TmpRecStnIdOnly(stnId.get, month, day, tmpC)
        else if((stnId isDefined) && (wbanId isDefined))
          new TmpRecStnAndWbanId(stnId.get, wbanId.get, month, day, tmpC)
        else if((stnId isEmpty) && (wbanId isDefined))
          new TmpRecWbanIdOnly(wbanId.get, month, day, tmpC)
        else
          new TmpRecErroneous(s"Erroneous Rec: ${xs.mkString(", ")}, STNID & WBAN Id is missing!")

      }
      catch {
        case ex: NumberFormatException => new TmpRecErroneous(s"Erroneous Record: [${xs.mkString(", ")}]!")
        case ex: ArrayIndexOutOfBoundsException => new TmpRecErroneous("xs[" + xs.mkString(", ") + "] Cannot Split Up to 5 Elements! >> ArrayIndexOutOfBoundsEx!" )
        case ex: Exception => new TmpRecErroneous(s"Erroneous Record: [${xs.mkString(", ")}]! Missing Temperature 9999.9F! ")
        case _: Throwable => new TmpRecErroneous(s"Unkown Error For Record: [${xs.mkString(", ")}]!")
      }

    }
  }


  def tmpRecGroupWithStations( year: Int,
                               stnIdOnlyMap: scala.collection.immutable.HashMap[Int, Station],
                               wbanIdOnlyMap: scala.collection.immutable.HashMap[Int, Station],
                               stnAndWbanMap: scala.collection.immutable.HashMap[(Int, Int), Station]): Flow[TemperatureRecord, Triplet, NotUsed] = {

    Flow[TemperatureRecord].map{tmpRec =>

      tmpRec match {

        case TmpRecStnIdOnly(stnId, month, day, tmpC) => {

          try {

            val station = stnIdOnlyMap.apply(stnId)
            println(s"Station Found: ${getStation(station)}")


            val vt = new ValidTriplet(LocalDate.of(year, month, day), new Location(station.lat, station.lon), tmpC)
            println(s"${vt.date.toString}, ${vt.location.lat}, ${vt.location.lon}, ${vt.temperatureCelcious}")

            vt
          }
          catch {
            case ex: NoSuchElementException => new InvalidTriplet(s"Station [STN_ID: ${stnId}, WBAN_ID: NULL] wasn't Found in STN_ID_Stations!")
            case th: Throwable => {
              new InvalidTriplet(th.getMessage)
            }
          }
        }

        case TmpRecWbanIdOnly(wbanId, month, day, tmpC) => {

          try {

            val station = wbanIdOnlyMap.apply(wbanId)
            println(s"Station Found: ${getStation(station)}")


            val vt = new ValidTriplet(LocalDate.of(year, month, day), new Location(station.lat, station.lon), tmpC)
            println(s"${vt.date.toString}, ${vt.location.lat}, ${vt.location.lon}, ${vt.temperatureCelcious}")

            vt

          }
          catch {
            case ex: NoSuchElementException => new InvalidTriplet(s"Station [STN_ID: NULL, WBAN_ID: ${wbanId}] wasn't Found in WBAN_ID_Stations!")
            case th: Throwable => {
              new InvalidTriplet(th.getMessage)
            }
          }
        }

        case TmpRecStnAndWbanId(stnId, wban, month, day, tmpC) => {
          try {

            val station = stnAndWbanMap.apply((stnId, wban))
            println(s"Station Found: ${getStation(station)}")


            val vt = new ValidTriplet(LocalDate.of(year, month, day), new Location(station.lat, station.lon), tmpC)
            println(s"${vt.date.toString}, ${vt.location.lat}, ${vt.location.lon}, ${vt.temperatureCelcious}")

            vt
          }
          catch {
            case ex: NoSuchElementException => new InvalidTriplet(s"Station [Id: ${stnId}, WBAN_ID: ${wban}] wasn't Found in STN_WBAN_Stations!")
            case th: Throwable => {
              new InvalidTriplet(th.getMessage)
            }
          }
        }

        case TmpRecErroneous(msg: String) => {
          new InvalidTriplet("Forwarding Erroneous Msg >> " + msg)
        }

        case _ => new InvalidTriplet("Should Not Reach Here!")
      }

    }

  }


  def temperatureRecToOutputPort(temperatureRecord: TemperatureRecord): Int = {
    temperatureRecord match {
      case TmpRecStnIdOnly(_, _, _, _)        => 0
      case TmpRecWbanIdOnly(_, _, _, _)       => 1
      case TmpRecStnAndWbanId(_, _, _, _, _)  => 2
      case TmpRecErroneous(msg)               => 3
    }
  }


  def stationToOutputPort(station: Station): Int = {
    if(station.stnId.isDefined && station.wbanId.isEmpty)
      0
    else if(station.stnId.isEmpty && station.wbanId.isDefined)
      1
    else
      2
  }


  def gTemperatures(temperatureFile: String,
                    year: Int,
                    sinkSeqTmp_STN_ID_ONLY  : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet],
                    sinkSeqTmp_WBAN_ID_ONLY : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet],
                    sinkSeqTmp_BOTH_STN_WBAN: Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet],
                    sinkSeqTmpErroneous     : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet],
                    stnIdMap                : scala.collection.immutable.HashMap[Int, Station],
                    wbanIdMap               : scala.collection.immutable.HashMap[Int, Station],
                    stnWbanMap              : scala.collection.immutable.HashMap[(Int, Int), Station]
                   ) = RunnableGraph.fromGraph(GraphDSL.create(sinkSeqTmp_STN_ID_ONLY, sinkSeqTmp_WBAN_ID_ONLY, sinkSeqTmp_BOTH_STN_WBAN, sinkSeqTmpErroneous)((_ , _, _, _)){

    implicit b: GraphDSL.Builder[(Future[Seq[Triplet]], Future[Seq[Triplet]], Future[Seq[Triplet]], Future[Seq[Triplet]])] =>

      (sinkSTN_ONLY, sinkWBAN_ONLY, sink_BOTH, sink_Erroneous) =>

        import GraphDSL.Implicits._

        //Source
        val source: Outlet[String] = b.add(Source.fromIterator(() => stringIteratorFromFile(temperatureFile))).out

        //Flows
        val f1: FlowShape[String, TemperatureRecord] = b.add(linesToTemperatures)

        val f2: FlowShape[TemperatureRecord, Triplet] = b.add(tmpRecGroupWithStations(year, stnIdMap, wbanIdMap, stnWbanMap) )
        val f3: FlowShape[TemperatureRecord, Triplet] = b.add(tmpRecGroupWithStations(year, stnIdMap, wbanIdMap, stnWbanMap) )
        val f4: FlowShape[TemperatureRecord, Triplet] = b.add(tmpRecGroupWithStations(year, stnIdMap, wbanIdMap, stnWbanMap) )
        val f5: FlowShape[TemperatureRecord, Triplet] = b.add(tmpRecGroupWithStations(year, stnIdMap, wbanIdMap, stnWbanMap) )

        //Partitioner
        val partitioner: UniformFanOutShape[TemperatureRecord, TemperatureRecord] = b.add(Partition[TemperatureRecord](4, tmpRec => temperatureRecToOutputPort(tmpRec)))


        //Graph
        source ~> f1 ~> partitioner

        //Has STN_ID Only!
        partitioner.out(0) ~> f2 ~> sinkSTN_ONLY

        //Has WBAN_ID Only!
        partitioner.out(1) ~> f3 ~> sinkWBAN_ONLY

        //Has Both STN & WBAN Identifier!
        partitioner.out(2) ~> f4 ~> sink_BOTH

        //Connect the Erroneous Data to Sink
        partitioner.out(3) ~> f5 ~> sink_Erroneous

        ClosedShape
  })



  def gStations(stationsFile: String,
                sinkSTN_ID  : Sink[Station, Future[Seq[Station]]],
                sinkWBAN_ID : Sink[Station, Future[Seq[Station]]],
                sinkSTNWBAN_ID : Sink[Station, Future[Seq[Station]]]
               ) = RunnableGraph.fromGraph(GraphDSL.create(sinkSTN_ID, sinkWBAN_ID, sinkSTNWBAN_ID)((_ , _, _))
  { implicit b: GraphDSL.Builder[(Future[Seq[Station]], Future[Seq[Station]], Future[Seq[Station]])] =>

    (sinkSTN_ONLY, sinkWBAN_ONLY, sink_BOTH) =>

      import GraphDSL.Implicits._

      //Source
      val source: Outlet[String] = b.add(Source.fromIterator(() => stringIteratorFromFile(stationsFile))).out

      //Flows
      val f1: FlowShape[String, Station] = b.add(linesToStations)

      //Partitioner
      val partitioner: UniformFanOutShape[Station, Station] = b.add(Partition[Station](3, station =>  stationToOutputPort(station)))


      //Graph
      source ~> f1 ~> partitioner

      //Has STN_ID Only!
      partitioner.out(0) ~> sinkSTN_ONLY

      //Has WBAN_ID Only!
      partitioner.out(1) ~> sinkWBAN_ONLY

      //Has Both STN & WBAN Identifier!
      partitioner.out(2) ~> sink_BOTH


      ClosedShape
  })


  def PrintStations[A](x: A) = {
    x match {
      case Station(stnId, wbanId, lat, lon) => println(s"${stnId.getOrElse("NULL")}, ${wbanId.getOrElse("NULL")}, $lat, $lon")
      case x => println(s"No Idea what: $x is!!!")
    }
  }


  def getStation[A](x: A): String = {
    x match {
      case Station(stnId, wbanId, lat, lon) => s"${stnId.getOrElse("NULL")}, ${wbanId.getOrElse("NULL")}, $lat, $lon"
      case x => s"No Idea what: $x is!!!"
    }
  }



  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {


    //Sinks
    val sinkSeqStation_STN_ID_ONLY: Sink[Station, Future[Seq[Station]]] = Sink.seq[Station]
    val sinkSeqStation_WBAN_ID_ONLY: Sink[Station, Future[Seq[Station]]] = Sink.seq[Station]
    val sinkSeqStation_BOTH_STD_WBAN: Sink[Station, Future[Seq[Station]]] = Sink.seq[Station]


    val t0 = System.nanoTime()

    val (stnIdSeqF, wbanIdSeqF, stnWbanSeqF) = gStations(stationsFile,
      sinkSeqStation_STN_ID_ONLY,
      sinkSeqStation_WBAN_ID_ONLY,
      sinkSeqStation_BOTH_STD_WBAN).run()

    val outerRes = for{
      stnIdSeq <- stnIdSeqF
      wbanIdSeq <- wbanIdSeqF
      stnWbanSeq <- stnWbanSeqF
    }yield{

      val stnIdMap: scala.collection.immutable.HashMap[Int, Station] = stnIdSeq.map(station => station.stnId.get -> station)(collection.breakOut)
      val wbanIdMap: scala.collection.immutable.HashMap[Int, Station] = wbanIdSeq.map(station => station.wbanId.get -> station)(collection.breakOut)
      val stnWbanMap: scala.collection.immutable.HashMap[(Int, Int), Station] = stnWbanSeq.map(station => (station.stnId.get, station.wbanId.get) -> station)(collection.breakOut)

      println()
      stnIdMap.foreach(x => println(s"${x._1} -> ${getStation(x._2)}"))
      println(s"Station Id Only: ${stnIdMap.size}\n")

      println()
      wbanIdMap.foreach(x => println(s"${x._1} -> ${getStation(x._2)}"))
      println(s"Wban Id Only: ${wbanIdMap.size}\n")


      println()
      stnWbanMap.foreach(x => println(s"${x._1} -> ${getStation(x._2)}"))
      println(s"Station & Wban: ${stnWbanMap.size}\n")

      println(s"All Together: ${stnIdMap.size + wbanIdMap.size + stnWbanMap.size}")


      //Sinks
      val sinkSeqTmp_STN_ID_ONLY  : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet]
      val sinkSeqTmp_WBAN_ID_ONLY : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet]
      val sinkSeqTmp_BOTH_STN_WBAN: Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet]
      val sinkSeqTmpErroneous     : Sink[Triplet, Future[Seq[Triplet]]] = Sink.seq[Triplet]


      val (stn_Id_TripleF, wban_Id_TripleF, stn_Wban_TripleF, erroneousF) = gTemperatures(temperaturesFile,
        year,
        sinkSeqTmp_STN_ID_ONLY,
        sinkSeqTmp_WBAN_ID_ONLY,
        sinkSeqTmp_BOTH_STN_WBAN,
        sinkSeqTmpErroneous,
        stnIdMap,
        wbanIdMap,
        stnWbanMap).run()


      val stnIdOnlyTripletsF = stn_Id_TripleF.map(seqTriplet => seqTriplet.partition(triplet => triplet.isInstanceOf[ValidTriplet]))
      val wbanIdOnlyTripletsF = wban_Id_TripleF.map(seqTriplet => seqTriplet.partition(triplet => triplet.isInstanceOf[ValidTriplet]))
      val stnAndWbanTripletsF = stn_Wban_TripleF.map(seqTriplet => seqTriplet.partition(triplet => triplet.isInstanceOf[ValidTriplet]))


      val innerRes = for{
        (stnIdValid_TripleSeq, stnIdInvalid_TripleSeq) <- stnIdOnlyTripletsF
        (wbanIdValid_TripleSeq, wbanIdInvalid_TripleSeq) <- wbanIdOnlyTripletsF
        (stnAndWbanValid_TripleSeq, stnAndWbanInvalid_TripleSeq) <- stnAndWbanTripletsF
        erroneousSeq <- erroneousF
      }yield {


        val allValidTripletsSeq = stnIdValid_TripleSeq ++ wbanIdValid_TripleSeq ++ stnAndWbanValid_TripleSeq
        val allInvalidTripletsSeq = stnIdInvalid_TripleSeq ++ wbanIdInvalid_TripleSeq ++ stnAndWbanInvalid_TripleSeq ++ erroneousSeq


        println(s"\nAll Valid Triples of this File are: ${allValidTripletsSeq.size}\n")
        println(s"\nAll Invalid Triples of this File are: ${allInvalidTripletsSeq.size}\n")


        println("\nStation Triples Only: \n")
        stnIdValid_TripleSeq.take(10).foreach(x => x match {
          case x: ValidTriplet => println(x.date.toString + s", (${x.location.lon}, ${x.location.lat}), ${x.temperatureCelcious}")
          case x: InvalidTriplet => println(x.msg)
        }
        )

        println("\nWBAN Triples Only: \n")
        wbanIdValid_TripleSeq.take(10).foreach(x => x match {
          case x: ValidTriplet => println(x.date.toString + s", (${x.location.lon}, ${x.location.lat}), ${x.temperatureCelcious}")
          case x: InvalidTriplet => println(x.msg)
        }
        )

        println("\nStation & WBAN Triples Only: \n")
        stnAndWbanValid_TripleSeq.take(10).foreach(x => x match {
          case x: ValidTriplet => println(x.date.toString + s", (${x.location.lon}, ${x.location.lat}), ${x.temperatureCelcious}")
          case x: InvalidTriplet => println(x.msg)
        }
        )

        println("\nErroneous Triples Only: \n")
        allInvalidTripletsSeq.take(10).foreach(x => x match {
          case x: InvalidTriplet => println(x.msg)
        }
        )


        val valTriplIter = allValidTripletsSeq.map{x =>
          x match {
            case ValidTriplet(date, loc, tmp) => (date, loc, tmp)
          }
        }.toIterable


        valTriplIter

      }


      innerRes
    }



    val finalRes = Await.result( Await.result(outerRes, 5 minutes) , 10 minutes)


    val t1 = System.nanoTime()
    println("\nElapsed time: " + (t1 - t0) + "ns")


    finalRes

  }


  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {

    val t0 = System.nanoTime()


    val source = Source.fromIterator[(LocalDate, Location, Double)](() => records.toIterator)


    val groupings = source.groupBy[(Int, Location)](Int.MaxValue , a => (a._1.getYear, a._2))



    val reducedGroups = groupings.fold(Location(0.0, 0.0), 0.0, 0) {
      (zero: (Location, Double, Int), x: (LocalDate, Location, Double)) =>
        (x._2, zero._2 + x._3, zero._3 + 1)
    }.mergeSubstreams



    val res = reducedGroups.map{case (loc: Location, allTmp: Double, count: Int) =>
                                    (loc, allTmp / count)
                               }


    val fSeqRes = res.runWith(Sink.seq[(Location, Double)])(materializer)



    val finalRes = Await.result(fSeqRes, 10 minutes)
    val t1 = System.nanoTime()

    println("\nAverage Temperatures Per Year and Location\n")
    finalRes.take(10).foreach(x => println(s"${x._1}, ${x._2}"))
    println("\n")


    println("\nElapsed time: " + (t1 - t0) + "ns")


    finalRes.toIterable

  }


  //actorSystem.terminate()

}


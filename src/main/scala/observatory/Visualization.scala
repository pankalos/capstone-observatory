package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.parallel.immutable.ParRange
import scala.math._
import scala.language.postfixOps
import scala.collection.parallel.mutable._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /*
  * Calculate the distance between 2 locations in the earth surface.
  *
  */
  def greatCircleDistance(location1: Location, location2: Location) : Double = {

    val lat1rad = location1.lat.toRadians
    val lon1rad = location1.lon.toRadians

    val lat2rad = location2.lat.toRadians
    val lon2rad = location2.lon.toRadians


    val ds = acos( (sin(lat1rad) * sin(lat2rad)) + (cos(lat1rad) * cos(lat2rad) * cos(abs(lon1rad - lon2rad)) )  )


    if(cos(ds) >= 0.99999999) //The 2 Locations are closer than 1Km.
      0.0
    else
      6371 * ds           //Radius of the Earth 6371 KM.

  }


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {


    def predictTemp2(temperatureList: List[(Location, Double)], location: Location, p: Double, flag: Int): (Double, Double, Int)= {

      temperatureList match {

        case List() => (0.0, 0.0, 0)

        case x :: xs => {

          val ds = greatCircleDistance(x._1, location)

          if(ds != 0.0){

            val res = predictTemp2(xs, location, p, flag)

            if(res._3 == 0){
              val divider = pow(ds, p)      // ... % (x - xi) ^ p

              val sum1 = x._2 / divider
              val sum2 = 1 / divider

              (sum1 + res._1, sum2 + res._2, flag)

            }
            else
              res

          }
          else
            (x._2, x._2, 1)
        }

      }

    }



    val tmpList = temperatures.toList

                                //Let's try p = 2!
    val res = predictTemp2(tmpList, location, 2.0, 0)



    if(res._3 == 1)
      res._1
    else
      res._1 / res._2

  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {


    def minAndMaxBoundaries(points: List[(Double, Color)],
                            value: Double,
                            minB: Double, minColor: Color,
                            maxB: Double, maxColor: Color): (Double, Color, Double, Color) = {

      points match {

        case List() => (minB, minColor, maxB, maxColor)

        case x :: xs => {

          if(x._1 < value){
            if(x._1 > minB)     //Move the Left Bounndary to the Right.
              minAndMaxBoundaries(xs, value, x._1, x._2, maxB, maxColor)
            else
              minAndMaxBoundaries(xs, value, minB, minColor, maxB, maxColor)
          }
          else if(x._1 > value){
            if(maxB > x._1)     //Move the Right Bounndary to the Left.
              minAndMaxBoundaries(xs, value, minB, minColor, x._1, x._2)
            else
              minAndMaxBoundaries(xs, value, minB, minColor, maxB, maxColor)
          }
          else
            (x._1, x._2, x._1, x._2)
        }

      }


    }


    def lerpRGB(c1: Color, c2: Color, t: Double): Color = {

      //c = c1 + (c2 - c1) * t

      //println(s"(${c1.red}, ${c1.green}, ${c1.blue})  ---  (${c2.red}, ${c2.green}, ${c2.blue})")

      val red   = c1.red   +   round((c2.red   -  c1.red)   * t).toInt
      val green = c1.green +   round((c2.green -  c1.green) * t).toInt
      val blue  = c1.blue  +   round((c2.blue  -  c1.blue)  * t).toInt


      if(red < 0 || red > 255 || green < 0 || green > 255 || blue < 0 || blue > 255){
        println( "(" + c1.red + ",  " + c1.green + ", " + c1.blue + "), " + "(" + c2.red + ",  " + c2.green + ", " + c2.blue + s"), t: ${t} ==> " + "(" + red + ",  " + green + ", " + blue + ")!!!!")
      }


      new Color(red, green, blue)
    }



    val pointsL = points.toList

    var (minB, minColor, maxB, maxColor) = minAndMaxBoundaries(pointsL, value, Double.MinValue, new Color(-900, -900, -900), Double.MaxValue, new Color(-900, -900, -900))


    var finalValue = value


    //val str = pointsL.map(x => x._1).mkString("[", ", ", "]")
    //println(str + " ==> " + value)

    if(minB == Double.MinValue) {
      //println(s"[${minB} => ${maxB} ... ${value} => ${maxB} ... ${maxB}]\n")
      minB = maxB
      finalValue = maxB
      minColor = maxColor
    }
    else if (maxB == Double.MaxValue) {
      //println(s"[${minB} ... ${value} => ${minB}... ${maxB} => ${minB}]\n")
      finalValue = minB
      maxB = minB
      maxColor = minColor
    }
    else
      ()
      //println(s"[${minB} ... ${value} ... ${maxB}]\n")



    var t = 0.0

    if(maxB != minB) {
      // t = (x - a) / (b - a)
      t = (finalValue - minB) / (maxB - minB)
    }

    if(t < 0.0 || t > 1.0)
      println("(" + finalValue + " - " + minB + ") / (" + maxB + " - " + minB + ") ==> " + t + "\n")

    lerpRGB(minColor, maxColor, t)

  }


  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {


    val pixelParRange = new ParRange(0 until (360 * 180))
                            .map{
                              i => {
                                val lat = 90 - (i / 360)
                                val lon = (i % 360) - 180

                                val predTemp = predictTemperature(temperatures, Location(lat, lon))
                                val predColor = interpolateColor(colors, predTemp)

                                Pixel.apply(predColor.red, predColor.green, predColor.blue, 255)
                              }
                            }



    val pixelArray: Array[Pixel] = pixelParRange.toArray


    Image.apply(360, 180, pixelArray)


  }




}


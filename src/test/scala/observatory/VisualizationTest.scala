package observatory


import java.io.File

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers
import Visualization._


@RunWith(classOf[JUnitRunner])
class VisualizationTest extends FunSuite with Checkers {

  test("predictTemperature: some point closer") {
    val location1 = Location(1,1)
    val temp1 = 10d
    val location2 = Location(-10,-10)
    val temp2 = 50d
    val list = List(
      (location1, temp1),
      (location2, temp2)
    )
    val result = Visualization.predictTemperature(list, Location(0, 0))
    assert(temp1 - result < temp2 - result)
  }


  test("Visualization Test Image") {

    val temperatures : Iterable[(Location, Double)] = Iterable(   (Location(45.0, -90.0) , 1.0) , (Location(-45.0, 90.0) , -100.0), (Location(45.0, 90.0) , 60.0)  )
    val colors: Iterable[(Double, Color)] = Iterable( (1.0 , Color(255, 0, 0)) , (-100.0 , Color(0, 0, 255)), (60.0 , Color(160, 160, 160))  )

    val img = visualize(temperatures, colors)

    img.output(new File("/home/pankalos/Downloads/visualization.png"))

    assert(1 == 1)
  }

}




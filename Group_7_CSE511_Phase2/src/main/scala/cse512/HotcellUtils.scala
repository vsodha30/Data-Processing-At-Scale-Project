package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def isNeighbour (x1: Int, x2: Int, x3: Int, y1:Int, y2:Int, y3:Int): Boolean = {
    if(x1 == y1 && x2 == y2 && x3 == y3) {
      return false;
    }
    if(x1 <= y1 + 1 && x1 >= y1 - 1) {
      if(x2 <= y2 + 1 && x2 >= y2 - 1) {
        if(x3 <= y3 + 1 && x3 >= y3 - 1) {
          return true;
        }
      }
    }
    return false;
  }

  def total_neighbours(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    var corners = 0;
    if(x == minX || x == maxX) {
      corners += 1;
    }
    if(y == minY || y == maxY) {
      corners += 1;
    }
    if(z == minZ || z == maxZ) {
      corners += 1;
    }
    if(corners == 0) {
      return 26;
    }
    else if(corners == 1) {
      return 17;
    }
    else if(corners == 2) {
      return 11;
    }
    return 7;
  }

  def gvalue(x: Int, y: Int, z: Int, attr: Int, ncount: Int, neighbours: Int, xmean: Double, s: Double, numCells: Int): Double = {
    /*val nxmean = ncount.toDouble / neighbours.toDouble
    val ns = math.sqrt((ncount2 / neighbours) - (nxmean * nxmean))
    val nnum = ncount - (nxmean * neighbours)
    val ndeno = ns * math.sqrt(((neighbours * neighbours) - (neighbours * neighbours)) / (numCells - 1))*/
    val num = ncount - (xmean * neighbours)
    val deno = s * math.sqrt(((numCells * neighbours) - (neighbours * neighbours)) / (numCells - 1))
    return num / deno;
  }
  // YOU NEED TO CHANGE THIS PART
}

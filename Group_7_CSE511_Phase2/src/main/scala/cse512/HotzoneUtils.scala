package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectArray = queryRectangle.split(",")
    val pointArray = pointString.split(",")

    val rectx1 = rectArray(0).toDouble
    val recty1 = rectArray(1).toDouble
    val rectx2 = rectArray(2).toDouble
    val recty2 = rectArray(3).toDouble

    val pointx = pointArray(0).toDouble
    val pointy = pointArray(1).toDouble

    if (pointx >= rectx1 && pointx <= rectx2 && pointy >= recty1 && pointy <= recty2) {
      return true
    }
    return false;
  }

  // YOU NEED TO CHANGE THIS PART

}

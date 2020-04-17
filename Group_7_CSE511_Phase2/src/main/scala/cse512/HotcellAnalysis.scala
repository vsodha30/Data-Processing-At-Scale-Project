package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)


  pickupInfo.createOrReplaceTempView("input")
  pickupInfo.show()


  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.filter("x >= " + minX + " AND x <= " + maxX)
  pickupInfo.filter("y >= " + minY + " AND y <= " + maxY)
  pickupInfo.filter("z >= " + minZ + " AND z <= " + maxZ)

  spark.udf.register("is_neighbour", (x1:Int, x2:Int, x3:Int, y1:Int, y2:Int, y3:Int)=>(HotcellUtils.isNeighbour(x1,x2,x3,y1,y2,y3)))
  spark.udf.register("gvalue", (x: Int, y: Int, z: Int, attr: Int, ncount: Int, neighbours:Int, xmean: Double, s: Double, numCells: Int)=>(HotcellUtils.gvalue(x, y, z, attr, ncount, neighbours, xmean, s, numCells)))
  spark.udf.register("total_neighbours", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => (HotcellUtils.total_neighbours(x, y, z, minX, maxX, minY, maxY, minZ, maxZ)))
  // YOU NEED TO CHANGE THIS PART
  val attrDF = pickupInfo.groupBy("x","y","z").count().as("aggr")
  attrDF.createOrReplaceTempView("attr_table")
  val xmean = spark.sql("SELECT sum(attr_table.count) FROM attr_table").first().getLong(0).toDouble / numCells
  val s = math.sqrt((spark.sql("SELECT sum(power(attr_table.count, 2)) FROM attr_table").first().getDouble(0).toDouble / numCells) - (xmean * xmean))
  attrDF.show()

  val neighboursDf = spark.sql("SELECT t1.x as x, t1.y as y, t1.z as z, t1.count as attr, t2.x as nx, t2.y as ny, t2.z as nz, t2.count as nattr FROM attr_table as t1, attr_table as t2 WHERE is_neighbour(t1.x, t1.y, t1.z, t2.x, t2.y, t2.z)")
  neighboursDf.createOrReplaceTempView("processed_table")
  neighboursDf.show()

  val bounds = minX + ", " + maxX + ", " + minY + ", " + maxY + ", " + minZ + ", " + maxZ
  val aggrDF = spark.sql("SELECT x, y, z, attr, sum(nattr) as ncount, total_neighbours(x, y, z, " + bounds + ") as neighbours FROM processed_table GROUP BY x,y,z,attr")
  //sum(power(ncount, 2)) as ncount2,
  aggrDF.createOrReplaceTempView("aggr_table")
  aggrDF.show()



  val global_param = xmean + ", " + s + ", " + numCells
  val gDF = spark.sql("SELECT x, y, z, gvalue(x, y, z, attr, ncount, neighbours, " + global_param + ") as g FROM aggr_table ORDER BY g DESC")
  //, ncount2
  gDF.show()

  return gDF // YOU NEED TO CHANGE THIS PART
}
}

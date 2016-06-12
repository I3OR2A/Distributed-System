import org.apache.spark._

/**
  * Created by I3OR2A on 2016/5/28.
  */
object ProblemOne {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.out.println("Usage : <input> <output> <threshold>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val context = new SparkContext(conf)

    val textFile = context.textFile(args(0))
    textFile.flatMap(str => str.split("::|\\|")).map(word => (word, 1)).reduceByKey((x, y) => x + y).filter(_._2 >= args(2).toInt).saveAsTextFile(args(1))
  }
}

import org.apache.spark._

object WordCount{
  def main(args:Array[String]): Unit = {
    if (args.length != 2){
      System.out.println("Usage : <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val context = new SparkContext(conf)

    val textFile = context.textFile(args(0))
    textFile.flatMap(str => str.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).saveAsTextFile(args(1))
  }
}

//  spark-submit --class WordCount --master spark://cdc-course-05:10005 untitled.jar hdfs://cdc-course-05:8020/tmp/sherlock.txt hdfs://cdc-course-05:8020/result2

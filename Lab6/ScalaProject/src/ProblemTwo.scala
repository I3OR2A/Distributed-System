import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by I3OR2A on 2016/5/28.
  */
object ProblemTwo {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.out.println("Usage : <movies> <ratings> <output> <threshold>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val context = new SparkContext(conf)

    val text_movies = context.textFile(args(0))
    val split_movies = text_movies.map(line => line.split("::"))
    var tuple_movies = split_movies.map(array => (array(0), array(1)))

    val text_ratings = context.textFile(args(1))
    val split_ratings = text_ratings.map(line => line.split("::"))
    val tuple_ratings = split_ratings.map(array => (array(0), array(1), array(2), array(3)))
    val resize_ratings = tuple_ratings.map { case (user, movie, rating, timestamp) => (movie, (rating.toDouble, 1.0)) }
    val reduce_ratings = resize_ratings.reduceByKey((x, y) => (x._1.+(y._1), x._2.+(y._2)))
    val average_ratings = reduce_ratings.map(tuple => (tuple._1, tuple._2._1./(tuple._2._2)))

    val result_statisic = average_ratings.join(tuple_movies)
    val filter_statisic = result_statisic.filter(_._2._1.>=(args(3).toDouble))
    filter_statisic.saveAsTextFile(args(2))
  }
}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.math.log

case class Review(
                   star_rating: String,
                   review_length: String,
                   vine: Boolean,
                   verified_purchase: Boolean
                 )


object Main extends App {
//  val conf = new SparkConf()
//  conf.setAppName("SparkErdogan")
//  conf.setMaster("local[4]")
//  val spark = SparkSession.builder().config(conf).getOrCreate()
//  val sc = spark.sparkContext


  var conf = new SparkConf() // Spark configuration
    .setAppName("Erdogan decision tree project") // Application name
    .setMaster("local[4]") // Acts as a master node with 4 thread
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
//  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//  var dataFrame = sparkSession.read
//    .option("delimiter", "\t")
//    .option("header", "true")
//    //          .csv("/data/amazon-reduced/amazon_reviews_us_Health_Personal_Care_v1_00.tsv")
//    .csv("sample_us.tsv")
//    .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body", "total_votes", "helpful_votes") // select is a transformation
//
//  dataFrame.show(5)
//  dataFrame.printSchema()

  // Convert continuous to discrete values
  def parseReview(review: String): Review = {
    val parts = review.split("\t") // split the line on the tab character

    // Construct a Review object from the parts
    Review(
      if (parts(7).toInt < 3) "low" else "high", // star_rating is the 8th field in the row
      if (parts(13).length < 200) "short" else "long", // review_length
      if (parts(10) == "Y") true else false, // vine is the 11th field in the row
      if (parts(11) == "Y") true else false // verified_purchase is the 12th field in the row
    )
  }


  val reviews = sc.textFile("sample_us.tsv")
//  val rev = reviews.take(10)
//  rev.foreach(println)
  // Dropping the header
  val header = reviews.first()
  println("here header",header)
  val reviewsWithoutHeader = reviews.filter(row => row != header)

  // Now parse the reviews
  val parsedReviews = reviewsWithoutHeader.map(parseReview)
  parsedReviews.take(10).foreach(println)

  type AttributeId = String

  // Defining function to get attribute from Review object based on target
  def getAttribute(review: Review, target: String): String = target match {
    case "star_rating" => review.star_rating
    case "review_length" => review.review_length
    case "vine" => review.vine.toString
    case "verified_purchase" => review.verified_purchase.toString
    case _ => throw new IllegalArgumentException("Invalid target attribute")
  }

//  def entropy(data: RDD[Review], targetAttribute: String, totalDataPoints: Long): Double = {
//
//    // Function to compute entropy
//    def computeEntropy(count: Long): Double = {
//      val proportion = count.toDouble / totalDataPoints.toDouble
//      -proportion * (math.log(proportion) / math.log(2))
//    }
//
//    // Creating a paired RDD with attribute counts
//    val attributeCountsRDD = data.map(review => (getAttribute(review, targetAttribute), 1)).reduceByKey(_ + _)
//
//    // Calculate entropy for each distinct attribute value and sum them up
//    val totalEntropy = attributeCountsRDD.map { case (_, count) => computeEntropy(count) }.sum()
//
//    totalEntropy
//  }


    def entropy(data: RDD[Review], target: AttributeId, totalDataPoints: Long): Double = {

    // Function to compute entropy
    def computeEntropy(count: Long): Double = {
      val proportion = count.toDouble / totalDataPoints.toDouble
      -proportion * (math.log(proportion) / math.log(2))
    }


    // Creating a paired RDD
    val attributeCountsRDD = data.map(review => (getAttribute(review,target), 1)).reduceByKey(_ + _)
    // for example our target is review_length
    // Attribute: long has count: 14
    // Attribute: short has count: 35
    // RDD of tuples like [(long, 14), (short,35)]
    // attributeCountsRDD.take(10).foreach { case (attribute, count) =>
    // println(s"Attribute: $attribute has count: $count")
    // }

    // Calculate entropy for each distinct attribute value and sum them up
    val mappingEntropy = attributeCountsRDD.map { case (attribute, count) => (attribute, computeEntropy(count)) }
    // RDD of tuples like [Entropy(long, 14), (short,35)]
    // mappingEntropy.take(10).foreach { case (attribute, entropy) =>
    // println(s"Attribute: $attribute has entropy: $entropy")
    // }

    val totalEntropy = mappingEntropy.map { case (_, entropy) => entropy }.sum()
    // totalEntropy = Entropy(long, 14) + Entropy(short, 35) = 0.51 + 0.34 = 0.86

  // println(s"Total entropy: $totalEntropy")

    totalEntropy
  }

  val totalDataPoints = parsedReviews.count()
  println("total data points " +totalDataPoints)
  val targetEntropy = entropy(parsedReviews, "review_length", totalDataPoints)
  println("Entropy of target attribute: " + targetEntropy)

  // calculates the amount of information gained about the target attribute after performing a split on another attribute in a dataset.
  def IG(data: RDD[Review], target: String, attribute: String, initialEntropy: Double): Double = {

    // Total number of data points
    val totalDataCount = data.count().toDouble

    // Compute counts for each combination of target and attribute values
    // For example RDD with 3 data points [(5, true), (5, true), (3, false)]
    // Map each data point to a tuple with 1: [((5, true), 1), ((5, true), 1), ((3, false), 1)]
    // We then use reduceByKey(_ + _) to add up the 1's for each unique key, ending up with: [((5, true), 2), ((3, false), 1)].
    // for example we want to know how many instances have 'verified_purchase' as true and how many have 'verified_purchase' as false

    val groupMapped = data.map(review => ((getAttribute(review, target), getAttribute(review, attribute)), 1))
    // groupMapped.take(10).foreach(println)
    // groupMapped.take(10).foreach {
      // case ((targetValue, attributeValue), count) => println(s"Target Value: $targetValue, Attribute Value: $attributeValue, Count: $count")
    // }
    // Cache the data for faster access in later computations because we are going to use groupedCounts RDD multiple times.
    val groupedCounts = groupMapped.reduceByKey(_ + _).persist() // This will be used multiple times
    // groupedCounts.take(10).foreach(println)

    // Compute total counts for each attribute value
    // (short, 31)
    // (long, 3)
    // (long, 11)
    // (short, 4)
    val attributeValueMap = groupedCounts
      .map { case ((_, attributeValue), count) => (attributeValue, count) }
    // attributeValueMap.take(10).foreach(println)
    // (long, 14)
    // (short, 35)
    // Aggregate the counts for each unique combination.
    val attributeValueCounts = attributeValueMap.reduceByKey(_ + _)
     // attributeValueCounts.take(10).foreach(println)

    // (short, 0.41787590909343514)
    // (long, 0.24671922510575828)
    // (long, 0.4838379689848371)
    // (short, 0.29507835462164966)
    val entropyPerGroupMap = groupedCounts
      .map { case ((_, attributeValue), count) =>
        val proportion = count / totalDataCount
        (attributeValue, -proportion * log(proportion) / log(2))
      }
//    entropyPerGroupMap.take(10).foreach(println)

    // Compute entropy for each group
    // (long, 0.7305571940905954)
    // (short, 0.7129542637150847)
    val entropyPerGroup = entropyPerGroupMap
      .reduceByKey(_ + _) // Summing up entropies for the same attribute value
    // entropyPerGroup.take(10).foreach(println)
    // entropyPerGroup: [("True", 0.3), ("False", 0.4)]
    // attributeValueCounts: [("True", 50), ("False", 50)]
    // Joined RDD: [("True", (0.3, 50)), ("False", (0.4, 50))]
    // if totalDataCount is 100, the function multiplies 0.3 * 50 / 100 = 0.15
    // for "True" and 0.4 * 50 / 100 = 0.2
    // for "False".The result of the map operation is an RDD: [("True", 0.15), ("False", 0.2)]
    val entropyAfterSplit = entropyPerGroup
      .join(attributeValueCounts) // This will create RDD of type [(attributeValue, (entropy, attributeCount))]
      .map { case (_, (entropy, attributeCount)) =>
        entropy * attributeCount / totalDataCount
      }
    // 0.20873062688302726
    // 0.5092530455107748
    // entropyAfterSplit.take(10).foreach(println)

    // Compute weighted sum of entropies (total entropy ,0.717983672393802)
    val totalEntropyAfterSplit = entropyAfterSplit.sum()
    // println("total entropy ",totalEntropyAfterSplit)

    // Information gain is the difference between initial and new entropy
    val informationGain = initialEntropy - totalEntropyAfterSplit

    informationGain
  }


  val target = "star_rating"
  val attribute = "review_length"

  // Then you would first calculate the initial entropy:
  val initialEntropy = entropy(parsedReviews, target, parsedReviews.count())

  // Then, you can calculate the Information Gain:
  val informationGain = IG(parsedReviews, target, attribute, initialEntropy)

  println(s"Information Gain for attribute $attribute is: $informationGain")


}
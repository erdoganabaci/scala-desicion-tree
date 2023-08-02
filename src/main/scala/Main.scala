import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.math.log

object Utils {
  def getAttribute(review: Review, target: String): String = target match {
    case "star_rating" => review.star_rating
    case "helpful_vote" => review.helpful_vote
    case "review_length" => review.review_length
    case "vine" => review.vine.toString
    case "verified_purchase" => review.verified_purchase.toString
    case _ => throw new IllegalArgumentException("Invalid target attribute")
  }

  // Possible values for each attribute
  def possibleValues(attribute: String): Array[String] = {
    attribute match {
      case "star_rating" => Array("low", "high")
      case "review_length" => Array("short", "long")
      case "vine" => Array("true", "false")
      case "verified_purchase" => Array("true", "false")
      case "helpful_vote" => Array("Helpful", "Not Helpful")
      case _ => Array() //empty array, attribute is not recognized
    }
  }

}

case class Review(
                   star_rating: String,
                   review_length: String,
                   vine: Boolean,
                   verified_purchase: Boolean,
                   helpful_vote: String,
                 )


// Defining function to get attribute from Review object based on target


case class DecisionTree(targetAttribute: String) {
  var subTrees: Map[String, DecisionTree] = Map()

  def addSubTree(value: String, subTree: DecisionTree): Unit = {
    subTrees += (value -> subTree)
  }


  override def toString(): String = {
    var stringRepresentation = "\n DecisionTree("
    for ((value, subTree) <- subTrees) {
      stringRepresentation +=  "\t" + targetAttribute + " = " + value + " : " + subTree.toString() + " )\n"
    }
    stringRepresentation
  }

  def predict(review: Review): String = {
    val attributeValue = Utils.getAttribute(review, targetAttribute)
    val tree = subTrees(attributeValue)
    tree.predict(review)
  }

}

class LeafTree(value: String) extends DecisionTree(value) {
  override def toString(): String = "LeafTree: "+ value

  override def predict(review: Review): String = value
}



object Main extends App {
  type AttributeId = String

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
      if (parts(11) == "Y") true else false, // verified_purchase is the 12th field in the row,
      if (parts(8).toInt > 0) "Helpful" else "Not Helpful" // helpful_vote is the 9th field in the row
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
  parsedReviews.take(50).foreach(println)



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
    val attributeCountsRDD = data.map(review => (Utils.getAttribute(review,target), 1)).reduceByKey(_ + _)
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

    val groupMapped = data.map(review => ((Utils.getAttribute(review, target), Utils.getAttribute(review, attribute)), 1))
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


  val target = "helpful_vote"
  val attribute = "review_length"

  // Then you would first calculate the initial entropy:
  val initialEntropy = entropy(parsedReviews, target, parsedReviews.count())

  // Then, you can calculate the Information Gain:
  val informationGain = IG(parsedReviews, target, attribute, initialEntropy)

  println(s"Information Gain for attribute $attribute is: $informationGain")

  // Compute the most common value of the target in the dataset
  def mostCommonValue(data: RDD[Review], target: String, totalDataSize: Long): String = {
    if (totalDataSize > 0) {
      // Map the pair star_rating [("4", 1), ("5", 1), ("5", 1), ("4", 1), ("5", 1)]
      val attributeValueCountPairs = data.map(review => (Utils.getAttribute(review, target), 1))
      // Grouped together add count [("4", 2), ("5", 3)]
      val countByAttributeValue = attributeValueCountPairs.reduceByKey(_ + _)
      // Sort the pair in descending order by count.
      val sortedByCount = countByAttributeValue.sortBy(pair => pair._2, ascending = false)
      // Get the highest count
      val mostCommonPair = sortedByCount.first()
      // get the highest count "5", which is the most common star_rating in the data.
      val mostCommonValue = mostCommonPair._1

      mostCommonValue
    } else {
      "unknown" // If there is no data, we cannot determine the most common attribute
    }
  }

  val mostCommonStarRating = mostCommonValue(parsedReviews, "star_rating", parsedReviews.count())
  println(s"The most common star rating is: $mostCommonStarRating")


  def ID3(data: RDD[Review], target: String, attributes: Array[String], threshold: Int): DecisionTree = {

    // Total number of data points
    val totalDataSize = data.count()

    // Stop condition for the recursion.
    // 1) If data size is less than threshold.
    // 2) If attributes list is empty.
    // 3) If entropy of the data is zero, meaning all the data points belong to the same class.
    // Return a leaf node with the most common value in the data.
    if (totalDataSize < threshold || attributes.isEmpty || entropy(data, target, totalDataSize) == 0) {
      new LeafTree(mostCommonValue(data, target, totalDataSize))
    }
    else {
      // Compute initial entropy before the attribute selection
      val initialEntropy = entropy(data, target, totalDataSize)

      // Find the attribute that provides the maximum information gain when splitting the data based on it.
      // Information gain for an attribute is computed as difference between current entropy and weighted entropy after the split.
      val attributeIGPairs = attributes.map(attribute => (IG(data, target, attribute, initialEntropy), attribute))

      // Select attribute with maximum information gain
      val attributeWithMaxIG = attributeIGPairs.maxBy(_._1)._2

      // Create a new decision tree node with the selected attribute
      val tree = new DecisionTree(attributeWithMaxIG)

      // For each possible value of the chosen attribute, create a new branch in the tree
      for (v <- Utils.possibleValues(attributeWithMaxIG)) {

        // Filter the data for the current branch
        val filtered = data.filter(review => Utils.getAttribute(review, attributeWithMaxIG) == v)

        // Call ID3 recursively for the filtered data and remaining attributes
        val subtree = ID3(filtered, target, attributes.filter(_ != attributeWithMaxIG), threshold)

        // Add the resulting subtree as a branch to the current tree node
        tree.addSubTree(v, subtree)
      }

      // Return the constructed decision tree
      tree
    }
  }


  val data_test = sc.parallelize(Array[Review](
    Review("low", "long", true, true, "Helpful"),
    Review("high", "short", false, true, "Helpful"),
    Review("low", "long", true, false, "Not Helpful"),
    Review("high", "long", false, false, "Not Helpful"),
    Review("low", "short", false, true, "Helpful")
  ))


//  val target_id3 = "helpful_vote"
//  val attributes = Array("star_rating", "review_length", "vine", "verified_purchase")
//  val threshold = 2
//  val decisionTree = ID3(data_test, target_id3, attributes, threshold)
//
//  val testData = Review("low", "short", true, false, "Helpful")
//  val result = decisionTree.predict(testData)
//
//  println(result) // Print the prediction result


  // Specify the weights
  val weights = Array(0.8, 0.2)

  // Use `randomSplit` to split the RDD into training and testing RDDs
  val Array(trainReviewsRDD, testReviewsRDD) = parsedReviews.randomSplit(weights, 12345L)

  // Persist to use them multiple times data in memory for faster access
  trainReviewsRDD.persist()
  testReviewsRDD.persist()

  val target_id3 = "helpful_vote"
  val attributes = Array("star_rating", "review_length", "vine", "verified_purchase")
  val threshold = 2
   val decisionTree = ID3(trainReviewsRDD, target_id3, attributes, threshold)

  // Predicting results for all reviews in the test data
  val results = testReviewsRDD.map(review => decisionTree.predict(review))

  // Collect results to the driver node
  val collectedResults = results.collect()

  // Print the results
  collectedResults.foreach(println)


}
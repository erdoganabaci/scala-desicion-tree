import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.math.log
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

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

  var conf = new SparkConf() // Spark configuration
    .setAppName("Erdogan decision tree project") // Application name
    .setMaster("local[4]") // Acts as a master node with 4 thread
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

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
//  val reviews = sc.textFile("amazon_reviews_us_Camera_v1_00.tsv")
  val reviewsWithIndex = reviews.zipWithIndex()
  val reviewsWithoutHeader = reviewsWithIndex.filter { case (_, index) => index != 0 }.map(_._1)

  val parsedReviews = reviewsWithoutHeader.map(parseReview)
  parsedReviews.take(50).foreach(println)

    def entropy(data: RDD[Review], target: AttributeId, totalDataPoints: Long): Double = {
    // Function to compute entropy
    def computeEntropy(count: Long): Double = {
      val proportion = count.toDouble / totalDataPoints.toDouble
      -proportion * (math.log(proportion) / math.log(2))
    }
    // Creating a paired RDD
    val attributeCountsRDD = data.map(review => (Utils.getAttribute(review,target), 1)).reduceByKey(_ + _)
    // Calculate entropy for each distinct attribute value and sum them up
    val mappingEntropy = attributeCountsRDD.map { case (attribute, count) => (attribute, computeEntropy(count)) }
    val totalEntropy = mappingEntropy.map { case (_, entropy) => entropy }.sum()
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
    val groupMapped = data.map(review => ((Utils.getAttribute(review, target), Utils.getAttribute(review, attribute)), 1))
    // Cache the data for faster access in later computations because we are going to use groupedCounts RDD multiple times.
    val groupedCounts = groupMapped.reduceByKey(_ + _).persist() // This will be used multiple times
    val attributeValueMap = groupedCounts
      .map { case ((_, attributeValue), count) => (attributeValue, count) }
    // Aggregate the counts for each unique combination.
    val attributeValueCounts = attributeValueMap.reduceByKey(_ + _)
    val entropyPerGroupMap = groupedCounts
      .map { case ((_, attributeValue), count) =>
        val proportion = count / totalDataCount
        (attributeValue, -proportion * log(proportion) / log(2))
      }
    val entropyPerGroup = entropyPerGroupMap
      .reduceByKey(_ + _) // Summing up entropies for the same attribute value
    val entropyAfterSplit = entropyPerGroup
      .join(attributeValueCounts) // It will create RDD of type [(attributeValue, (entropy, attributeCount))]
      .map { case (_, (entropy, attributeCount)) =>
        entropy * attributeCount / totalDataCount
      }
    // Compute weighted sum of entropies (total entropy ,0.717983672393802)
    val totalEntropyAfterSplit = entropyAfterSplit.sum()
    // Information gain is the difference between initial and new entropy
    val informationGain = initialEntropy - totalEntropyAfterSplit

    informationGain
  }

  val target = "helpful_vote"
  val attribute = "review_length"

  val initialEntropy = entropy(parsedReviews, target, parsedReviews.count())
  val informationGain = IG(parsedReviews, target, attribute, initialEntropy)

  println(s"Information Gain for attribute $attribute is: $informationGain")

  // Compute the most common value of the target in the dataset
  def mostCommonValue(data: RDD[Review], target: String, totalDataSize: Long): String = {
    if (totalDataSize > 0) {
      val attributeValueCountPairs = data.map(review => (Utils.getAttribute(review, target), 1))
      val countByAttributeValue = attributeValueCountPairs.reduceByKey(_ + _)
      val sortedByCount = countByAttributeValue.sortBy(pair => pair._2, ascending = false)
      val mostCommonPair = sortedByCount.first()
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
        // In the recursive ID3 algorithm, persisting the filtered data in memory prevents the repeated and potentially
        // inefficient recomputation of the filtering operation during each recursive call.
        filtered.persist()
        // Call ID3 recursively for the filtered data and remaining attributes
        val subtree = ID3(filtered, target, attributes.filter(_ != attributeWithMaxIG), threshold)
        // Add the resulting subtree as a branch to the current tree node
        tree.addSubTree(v, subtree)
      }
      // Return the constructed decision tree
      tree
    }
  }

  val weights = Array(0.8, 0.2)

  // Use `randomSplit` to split the RDD into training and testing RDDs
  val Array(trainReviewsRDD, testReviewsRDD) = parsedReviews.randomSplit(weights, 12345L)

  // Persist to use them multiple times data in memory for faster access
  trainReviewsRDD.persist()
  testReviewsRDD.persist()

  val target_id3 = "helpful_vote"
  val attributes = Array("star_rating", "review_length", "vine", "verified_purchase")
  val threshold = 500
  // make tree and compute execution time
  val startTime = System.currentTimeMillis()
  val decisionTree = ID3(trainReviewsRDD, target_id3, attributes, threshold)
  val executionTime = System.currentTimeMillis() - startTime

  // Making predictions on the test data
  val predictionsRDD = testReviewsRDD
  .filter(review => review != null) // Filtering out null values
  .map(review => decisionTree.predict(review))

  // Zipping the true values and predictions
  val trueValuesAndPredictions = testReviewsRDD.map(_.helpful_vote).zip(predictionsRDD)

  // Calculating accuracy
  val correctPredictionsCount = trueValuesAndPredictions.filter { case (trueValue, prediction) => trueValue == prediction }.count
  val accuracy = correctPredictionsCount.toDouble / testReviewsRDD.count

  println(s"Accuracy: $accuracy")
  print("exectionTime:",executionTime)

  // write results to file
  Files.write(Paths.get("results.txt"), (
      "accuracy:" + accuracy.toString + " \n" +
      "tree:" + decisionTree.toString + " \n").getBytes(StandardCharsets.UTF_8))
}
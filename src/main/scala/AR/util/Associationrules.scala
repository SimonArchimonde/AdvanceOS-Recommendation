package AR.util

import AssociationRules.RuleNewDef
import FPNewDef.FreqItemset
import org.apache.spark.rdd.RDD

/* Generates association rules from a `RDD[FreqItemset[Item]]`. This method only generates
  * association rules which have a single item as the consequent.
  */
class AssociationRules(private var minConfidence: Double) extends Serializable {

  /* Constructs a default instance with default parameters {minConfidence = 0.8}.
    */
  def this() = this(0.8)

  /* Sets the minimal confidence (default: `0.8`).
    */
  def setMinConfidence(minConfidence: Double): this.type = {
    require(
      minConfidence >= 0.0 && minConfidence <= 1.0,
      s"Minimal confidence must be in range [0, 1] but got ${minConfidence}"
    )
    this.minConfidence = minConfidence
    this
  }

  /* Computes the association rules with confidence above [[minConfidence]].
    * @param freqItemsets frequent itemset model obtained from FPGrowth
    * @return a `Set[Rule[Item]]` containing the association rules.
    */
  def run(freqItemsets: RDD[FreqItemset]): RDD[RuleNewDef] = {
    // For candidate rule X => Y, generate (X, (Y, freq(X union Y)))
    val candidates = freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item => //先做items是否是1-项集的判断，然后再生成规则
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
          case _ => None
        }
      }
    } //每个item是(antecedent.toSeq, (consequent.toSeq, itemset.freq))

    // Join to get (X, ((Y, freq(X union Y)), freq(X))), generate rules, and filter by confidence
    candidates
      .join(freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .filter(f => f._2._1._2.toDouble / f._2._2.toDouble >= minConfidence)
      .map { case (antecendent, ((consequent, freqUnion), freqAntecedent)) =>
        new RuleNewDef(
          antecendent.toArray.sorted,
          consequent.toArray,
          freqUnion.toDouble / freqAntecedent
        )
      }
  }

}

object AssociationRules {

  /* Like fpm.AssociatedRules.Rule
    * @param antecedent
    * @param consequent
    * @param confidence
    */
  class RuleNewDef(
      val antecedent: Array[Int],
      val consequent: Array[Int],
      val confidence: Double
  ) extends Serializable {

    require(
      antecedent.toSet.intersect(consequent.toSet).isEmpty, {
        val sharedItems = antecedent.toSet.intersect(consequent.toSet)
        s"A valid association rule must have disjoint antecedent and " +
          s"consequent but ${sharedItems} is present in both."
      }
    )

    override def toString: String = {
      s"${antecedent.mkString("{", ",", "}")} => " +
        s"${consequent.mkString("{", ",", "}")}: ${confidence}"
    }
  }

}

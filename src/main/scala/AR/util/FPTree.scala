package AR.util

import AR.util.FPTree.Node

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/* FP-Tree data structure used in FP-Growth.
  *
  * //  * @tparam T item type
  */
class FPTree extends Serializable {

  import FPTree._

  val root: Node = new Node(null)

  //  private val summaries: mutable.Map[T, Parents[T]] = mutable.Map.empty

  /* Adds a transaction with count. */
  def add(t: Iterable[Int], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val child = curr.children.getOrElseUpdate(
        item, {
          val newNode = new Node(curr)
          newNode.item = item
          newNode
        }
      )
      child.count += count
      curr = child
    }
    this
  }

  def extract(
      minCount: Long,
      validateSuffix: Int => Boolean = _ => true
  ): Iterator[(List[Int], Long)] = {
    val reFPTree = new ReFPTree()
    reFPTree
      .generateTree(root, validateSuffix)
      .traverse(minCount, validateSuffix)
  }
}

object FPTree {

  /* Representing a node in an FP-Tree. */
  class Node(val parent: Node) extends Serializable {
    var item: Int = _
    var count: Long = 0L
    val children: mutable.Map[Int, Node] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

}

class ReFPTree[T]() extends Serializable {

  import ReFPTree._

  private val summaries: mutable.Map[Int, Summary] = mutable.Map.empty

  def generateTree(
      node: Node,
      validateSuffix: Int => Boolean = _ => true,
      parent: ReNode = new ReNode(null)
  ): this.type = {
    node.children.foreach { case (item, node) =>
      val curr = new ReNode(parent)
      curr.item = item
      if (validateSuffix(item)) {
        val summary = summaries.getOrElseUpdate(item, new Summary)
        summary.parents.append((parent, node.count))
        summary.count += node.count
      }
      generateTree(node, validateSuffix, curr)
    }
    this
  }

  def project(reNode: ReNode): List[Int] = {
    var curr = reNode
    val path: ListBuffer[Int] = ListBuffer.empty
    while (!curr.isRoot) {
      path += curr.item
      curr = curr.parent
    }
    path.toList
  }

  def extractOnePath(
      minCount: Long,
      suffix: List[Int],
      count: Long,
      parent: ReNode
  ): ListBuffer[(List[Int], Long)] = {
    val partFreqSet = ListBuffer.empty[(List[Int], Long)]
    val prefix = project(parent)

    var combination = List(suffix)
    prefix.foreach(item => {
      combination = combination ::: combination.map(list => list :+ item)
    })
    combination.drop(1).map(list => partFreqSet.append((list, count)))
    partFreqSet
  }

  def extract(
      minCount: Long,
      suffix: List[Int],
      count: Long,
      parents: mutable.Map[Int, ListBuffer[(ReNode, Long)]]
  ): ListBuffer[(List[Int], Long)] = {

    val partFreqSet = ListBuffer.empty[(List[Int], Long)]
    val deepNodeID = parents.keys.max
    val deepNodes = parents(deepNodeID)
    val nSuffix = suffix :+ deepNodeID
    var nSuffix_count = 0L
    var countRoot = 0L
    var suffix_count = count
    val deepNodes_parents = mutable.Map.empty[Int, ListBuffer[(ReNode, Long)]]
    parents -= deepNodeID
    deepNodes.foreach { case (node, count) =>
      nSuffix_count += count
      if (!node.parent.isRoot) {
        val deep_nodes = deepNodes_parents.getOrElseUpdate(
          node.parent.item,
          ListBuffer.empty[(ReNode, Long)]
        )
        deep_nodes.append((node.parent, count))
        val nodes = parents.getOrElseUpdate(
          node.parent.item,
          ListBuffer.empty[(ReNode, Long)]
        )
        nodes.append((node.parent, count))
      } else {
        suffix_count -= count
        countRoot += count
      }
    }
    if (nSuffix_count >= minCount) {
      partFreqSet.append((nSuffix, nSuffix_count))
    }
    //backtraking suffix+maxparent
    nSuffix_count -= countRoot
    if (nSuffix_count >= minCount && nSuffix_count != 0) {
      partFreqSet ++= extract(
        minCount,
        nSuffix,
        nSuffix_count,
        deepNodes_parents
      )
    }
    //backtacking suffix without maxparent
    if (suffix_count >= minCount && suffix_count != 0) {
      partFreqSet ++= extract(minCount, suffix, suffix_count, parents)
    }
    partFreqSet
  }

  def traverse(
      minCount: Long,
      validateSuffix: Int => Boolean = _ => true
  ): Iterator[(List[Int], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item)) {
        val freqItemset = ListBuffer.empty[(List[Int], Long)]
        if (summary.parents.size == 1 && summary.count >= minCount) {
          val parent = summary.parents.head._1
          if (!parent.isRoot) {
            freqItemset ++= extractOnePath(
              minCount,
              List(item),
              summary.count,
              parent
            )
          }
          freqItemset.append((List(item), summary.count))
        } else if (summary.parents.size > 1 && summary.count >= minCount) {
          // 1-suffix, 1-freqItemSet
          freqItemset.append((List(item), summary.count))
          // 1-suffix, over 1-freqItemSet
          val parents = mutable.Map.empty[Int, ListBuffer[(ReNode, Long)]]
          var suffix_count = summary.count
          summary.parents.foreach { case (node, count) =>
            if (!node.isRoot) {
              val nodes = parents.getOrElseUpdate(
                node.item,
                ListBuffer.empty[(ReNode, Long)]
              )
              nodes.append((node, count))
            } else {
              suffix_count -= count
            }
          }
          // for suffix count > mincount, get its path
          if (suffix_count >= minCount) {
            freqItemset ++= extract(minCount, List(item), suffix_count, parents)
          }
        }
        freqItemset.toIterator
      } else {
        Iterator.empty
      }
    }
  }
}

object ReFPTree {

  class ReNode(val parent: ReNode) extends Serializable {
    var item: Int = _

    def isRoot: Boolean = parent == null
  }

  /* Parents of an item in an FP-Tree. */
  private class Summary extends Serializable {
    var count: Long = 0L
    val parents: ListBuffer[(ReNode, Long)] = ListBuffer.empty
  }

}

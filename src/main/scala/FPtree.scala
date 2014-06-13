/**
 * Created by jthalbert on 6/13/14.
 */
class FPtree {
  class Node(val itemName: String,
             val count: Int,
             val nodeLink: Node) {
      var children: Array[Node] = new Array[Node](0)
  }

  private val root: Node = null

  def preorder(visit: Node => Unit ) {
    def recur(n: Node) {
      visit(n)
      n.children.foreach(recur)
    }
    recur(root)
  }

  def postorder(visit: Node => Unit ) {
    def recur(n: Node) {
      n.children.foreach(recur)
      visit(n)
    }
    recur(root)
  }
}

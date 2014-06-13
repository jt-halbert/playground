import java.io.File
import scala.io.Source

/**
 * Created by jthalbert on 6/6/14.
 *
 * This is a playground to hold simple text processing functions
 */
object textProcessor {
  type Header = Map[String,String]

  def getFiles(f: File): Stream[File] =
    (f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFiles)
            else Stream.empty)).filter(f => f.isFile)

  //TODO: refine the try-catch to avoid lines we can't process
  def getLinesFromFile(f: File): Option[List[String]] = {
    require(f.isFile, "cannot be a directory.")
    try {
      val s = Source.fromFile(f)
      val lines = s.getLines().toList
      s.close() //Source doesn't automatically close a File when it is done reading.  This is dumb.
      Some(lines)
    } catch {
      case ex: Exception => println(ex + f.getAbsolutePath); None
    }
  }

  def getHeader(lines: List[String]): Header = {
    val p = (s: String) => !s.matches("^.*:.*$")
    assume(!p(lines.head), "Malformed grouping: leading with a nonheader field")
    def gather(ls: List[String]): List[String] = {
      if (ls.isEmpty) Nil
      else {
        val (left, right) = ls.span(x => !p(x))
        left.init ::: List((left.last :: right.takeWhile(p)).mkString(" ")) :::
          gather(right.dropWhile(p))
      }
    }

    def processToMap(ls: List[String]): Map[String, String] = {
      assume(ls.forall(x => !p(x)), "Malformed list: each element must begin with header field.")
      ls.map(s => {
        try {
          val pieces = s.split(":")
          (pieces(0).trim,pieces(1).trim)
        } catch {
          case ex: Exception => println(ex + s)
            ("","")
        }
      }).filter(t => !t._1.isEmpty && !t._2.isEmpty).toMap  //TODO: find a more idiomatic way to do this
    }
    processToMap(gather(lines.takeWhile(l => !l.isEmpty).toList))
  }

  def getKeySets(hs: Stream[Header]): Map[String, Int] = {
    hs.foldLeft(Map[String, Int]()) {
      (m,h) => {
        val keySetName = h.keys.toList.sorted.mkString(",")
        m.updated(keySetName, m.getOrElse(keySetName,0)+1)
      }
    }
  }

  def getFrequentItems(hs: Stream[Header]): List[(String, Int)] = {
    hs.flatMap(_.keys.toList).groupBy(w=>w).mapValues(_.size).toList.sortBy(_._2).reverse
  }

//  def getFrequentHeaderItems(hs: Stream[Header]): List[String, Int] ={
//    hs.foldLeft()
//  }
//  def keyStats(ld: Stream[Header]): Map[String, Int] = {
//
//  }

//  (m,h) => {
//    | val keySetName = h.keys.toList.sorted.mkString(",")
//    | m.updated(keySetName, m.getOrElse(keySetName,0)+1)
//    | }}
//java.io.Fi
//  def pullToAndFrom(lines: String): (String, List[String]) ={
//
//  }

}


import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text
import org.apache.hadoop.examples.terasort.TeraInputFormat

import scala.reflect.ClassTag

/**
 * A quick and dirty file utility.  First use case is loading files enron set into HDFS
 *
 * Created by jthalbert on 6/25/14.
 */
object HDFSFileService {
  private val conf = new Configuration
  //TODO: is it possible to make these settings hadoop version independent? lol.
  private val hadoopPrefix = sys.env("HADOOP_PREFIX")
  private val hdfsCoreSitePath = new Path(hadoopPrefix+"/etc/hadoop/" + "core-site.xml")
  private val hdfsHDFSSitePath = new Path(hadoopPrefix+"/etc/hadoop/" + "hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  private val fileSystem = FileSystem.get(conf)

  def ls(pathName: String): Unit = {
    val path = new Path(pathName)
    val fileStatuses = fileSystem.listFiles(path,true)
    while (fileStatuses.hasNext) {
      println(fileStatuses.next().getPath)
    }
  }

  def writeToSequenceFile(tuples: Seq[(String, String)], filename: String): Unit = {
    val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.keyClass(classOf[Text]),
      SequenceFile.Writer.valueClass(classOf[Text]),
      SequenceFile.Writer.file(new Path(filename)))
    for((k,v) <- tuples) {
      writer.append(new Text(k),new Text(v))
    }
    writer.close()
  }

  def writeAllFilesToSequenceFiles(path: File, filename: String, numPerOutFile: Int = 10000): Unit = {
    /**
     * The .toiterator in the following val was the solution to a memory leak (so to speak).  It takes the
     * Stream object and frees up the processed elements for the GC
     */
    val keyVals = textProcessor.getFiles(path).toIterator
      .map(textProcessor.getLinesFromFile).flatten
      .map(_.mkString("\n"))
      .map(em => (textProcessor.md5Hash(em), em))
    var i = 0
    for (group <- keyVals.grouped(numPerOutFile)) {
      writeToSequenceFile(group, filename+i)
      i+=1
    }
  }


}


import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text

import scala.reflect.ClassTag

/**
 * A quick and dirty file utility.  First use case is loading files enron set into HDFS
 *
 * Created by jthalbert on 6/25/14.
 */
object HDFSFileService {
  private val conf = new Configuration
  //TODO: is it possible to make these settings hadoop version independent?
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

  def writeToSequenceFile(tuples: List[(String, String)], filename: String): Unit = {
    val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.keyClass(classOf[Text]),
      SequenceFile.Writer.valueClass(classOf[Text]),
      SequenceFile.Writer.file(new Path(filename)))
    for((k,v) <- tuples) {
      writer.append(new Text(k),new Text(v))
    }
    writer.close()
  }

  def writeAllFilesToSequenceFile(path: File, filename: String): Unit = {
    val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.keyClass(classOf[Text]),
      SequenceFile.Writer.valueClass(classOf[Text]),
      SequenceFile.Writer.file(new Path(filename)))
    for ((k,v) <- textProcessor.getFiles(path)
      .map(textProcessor.getLinesFromFile).flatten
      .map(_.mkString("\n"))
      .map(em => (textProcessor.md5Hash(em),em))) {
      writer.append(new Text(k), new Text(v))
    }
    writer.close()
  }


}

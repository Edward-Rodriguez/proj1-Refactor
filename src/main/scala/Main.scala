package scala

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.conf.Configuration

object Main extends App {

  if (args.length != 2) {
    println("Usage: Main [input dir] [output dir]")
    System.exit(-1)
  }
  val conf: Configuration = new Configuration();

  val inputFilename = args(0)
  val outputFilename = args(1)
  val job = Job.getInstance(conf)

  job.setJarByClass(Main.getClass())
  job.setJobName("Cases Count By Country")
  job.setInputFormatClass(classOf[TextInputFormat])

  FileInputFormat.setInputPaths(job, new Path(inputFilename))
  FileOutputFormat.setOutputPath(job, new Path(outputFilename))

  job.setMapperClass(classOf[CountryMapper])
  job.setReducerClass(classOf[CountryReducer])

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])

  val success = job.waitForCompletion(true)
  // System.exit(if (success) 0 else 1)
  if (success) {
    val conf2: Configuration = new Configuration();
    val input = args(1)
    val output = args(1) + "/sortedResults"
    val job2 = Job.getInstance(conf2)

    job2.setJarByClass(Main.getClass())
    job2.setJobName("Top 20 total Cases by Country")
    job2.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job2, new Path(input))
    FileOutputFormat.setOutputPath(job2, new Path(output))

    job2.setMapperClass(classOf[KeyValueSwapMapper])
    job2.setReducerClass(classOf[TopNReducer])

    job2.setOutputKeyClass(classOf[IntWritable])
    job2.setOutputValueClass(classOf[Text])

    val success2 = job2.waitForCompletion(true)
    System.exit(if (success2) 0 else 1)
  }

  // part-r-00000
}

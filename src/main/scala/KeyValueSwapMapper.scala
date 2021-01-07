package scala

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class KeyValueSwapMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, IntWritable, Text]#Context
  ): Unit = {

    val totalCasesCount: IntWritable = new IntWritable();
    val countryName: Text = new Text();

    val line = value.toString
    val casesParsed = line.split("\\D+")
    val countryParsed = line.split("\\d+")

    totalCasesCount.set(casesParsed(1).toInt * -1) //total cases count
    countryName.set(countryParsed(0)) //country name field

    context.write(totalCasesCount, countryName)
  }
}

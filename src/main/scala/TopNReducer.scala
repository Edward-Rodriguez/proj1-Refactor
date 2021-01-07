package scala

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import scala.collection.immutable.ListMap

class TopNReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
  var count = 0;
  val limit = 20;

  override def reduce(
      key: IntWritable,
      values: java.lang.Iterable[Text],
      context: Reducer[IntWritable, Text, IntWritable, Text]#Context
  ): Unit = {

    val totalCasesCount: IntWritable = new IntWritable();
    val countryName: Text = new Text();
    totalCasesCount.set((-1) * key.get())
    values.forEach(countryName.set(_))

    if (count < limit) {
      context.write(totalCasesCount, new Text(countryName))
      count += 1
    }

  }
}

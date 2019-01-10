package mapreduce_test.problem1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem1_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
    * <BR01, 40>
    * <BR03, 20>
    * ...
    * */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}

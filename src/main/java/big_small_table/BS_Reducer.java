package big_small_table;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Create By meta On 19-1-7 下午9:33
 */
public class BS_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable summary = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        summary.set(sum);
        context.write(key, summary);
    }
}

package sum_avg_group.p4;

/*
 * Created by meta on 19-1-10 下午8:34
 *
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class p4_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0.0f;
        for (FloatWritable salary : values) {
            sum += salary.get();
        }
        context.write(new Text(key), new FloatWritable(sum));
    }
}

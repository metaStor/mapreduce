package sum_avg_group.p2;

/*
 * Created by meta on 19-1-10 下午10:40
 *
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class p2_Reducer extends Reducer<Text, FloatWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        float sum = 0.0f;
        float avg = 0.0f;
        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }
        avg = sum / count;
        context.write(key, new Text("Department's numbers: " + count + "\tAvg of Salary: " + avg));
    }
}

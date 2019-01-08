package counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * Create By meta On 19-1-8 下午2:48
 */

public class Counter_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Counter counters = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        if (values.length > 3) {
            counters = context.getCounter("ErrorAccount", "TooLong");
            counters.increment(1);
        } else if (values.length < 3) {
            counters = context.getCounter("ErrorAccount", "TooShort");
            counters.increment(1);
        }
    }
}


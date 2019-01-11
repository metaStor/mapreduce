package mapreduce_test.problem1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem1_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    /*
    * <BR01, 10>
    * <BR01, 30>
    * <BR03, 15>
    * <BR03, 11>
    *  ...
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String orderId = values[2];
        int count = Integer.parseInt(values[3]);
        context.write(new Text(orderId), new IntWritable(count));
    }
}

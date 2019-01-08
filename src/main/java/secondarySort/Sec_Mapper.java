package secondarySort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * Create By meta On 19-1-7 下午4:42
 */

public class Sec_Mapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {  // NullWritable占位符

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String[] fields = value.toString().split("\t");
        // 只有两个字段
        int field1 = Integer.parseInt(fields[0]);
        int field2 = Integer.parseInt(fields[1]);
        context.write(new IntPair(field1, field2), NullWritable.get());
    }
}

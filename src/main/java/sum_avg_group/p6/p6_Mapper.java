package sum_avg_group.p6;

/*
 * Created by meta on 19-1-10 下午8:33
 *
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class p6_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        // 规范数据
        if (values.length != 7) {
            return;
        }
        String id = values[0].trim();
        String superiorId = values[3].trim();  // it might is "".
        // 有上司的： id @ superiorId
        // 自己是boss： id @ id
        context.write(new IntWritable(0), new Text(id + "@" + (superiorId.equals("") ? id : superiorId)));
    }
}

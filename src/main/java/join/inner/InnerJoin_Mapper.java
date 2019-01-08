package join.inner;

/*
 * Created by meta on 19-1-6 下午10:31
 *
 */

import join.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class InnerJoin_Mapper extends Mapper<LongWritable, Text, TextPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        // 获取文件名
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (fileName.equals("data.txt")) {
            String[] values = value.toString().split("\t");
            // 规范数据
            if (values.length >= 3) {
                // key
                TextPair textPair = new TextPair(values[1], "1");  // data.txt的内容标记为1
                // 输出mapper阶段输出的数据
                context.write(textPair, new Text(values[0] + '\t' + values[2]));
            }
        }
        if (fileName.equals("info.txt")) {
            String[] values = value.toString().split("\t");
            // 规范数据
            if (values.length >= 2) {
                // key
                TextPair textPair = new TextPair(values[0], "0");  // info.txt的内容标记为0
                // 输出mapper阶段输出的数据
                context.write(textPair, new Text(values[1]));
            }
        }
        // 输出当前读取到的key，value
//        System.out.println(context.getCurrentKey().get());
//        System.out.println(context.getCurrentValue().toString());
    }
}

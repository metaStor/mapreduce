package mapreduce_test.problem3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Problem3 extends Configured implements Tool{

    public static class P1_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /*
         * <BR01, 1>
         * <BR01, 1>
         * <BR03, 1>
         * <BR03, 1>
         *  ...
         * */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 输入的是一行，用逗号分割
            String[] values = value.toString().split(",");
            // 得到id
            String orderId = values[1];
            // 写入为： <BR01, 1>等...
            context.write(new Text(orderId), new IntWritable(1));
        }
    }

    public static class Problem1_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /*
         * <BR01, [1, 1]> => <BR01, 2>
         * ...
         * */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // 累加同供应商产品的数目
            for (IntWritable value: values) {
                sum += value.get();
            }
            // 写入为： <BR01, 2>等...
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Jar class
        job.setJarByClass(Problem3.class);
        // Mapper
        job.setMapperClass(P1_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Reducer
        job.setReducerClass(Problem1_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // input and output
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Problem3(), args));
    }
}

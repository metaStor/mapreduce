package mapreduce_test.problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem1 extends Configured implements Tool{

    public int run(String[] strings) throws Exception {
        if (strings.length < 2) {
            System.err.println("Usage: <Input> <Output>");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Jar class
        job.setJarByClass(Problem1.class);
        // Mapper
        job.setMapperClass(Problem1_Mapper.class);
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
        System.exit(ToolRunner.run(new Problem1(), args));
    }
}

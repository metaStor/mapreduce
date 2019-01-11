package mapreduce_test.problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem2 extends Configured implements Tool{

    public int run(String[] strings) throws Exception {
//        if (strings.length < 3) {
//            System.err.println("Usage: <Vendors> <Products> <Output>");
//            System.exit(2);
//        }
        Configuration configuration = new Configuration();
//        configuration.set("cacheFile", "/home/meta/software/IdeaProjects/sh/src/main/java/mapreduce_test/data/Vendors.csv");
        Job job = Job.getInstance(configuration);
        // Jar class
        job.setJarByClass(Problem2.class);
        // Mapper
        job.setMapperClass(Problem2_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ProductText.class);
        // Reducer
        job.setReducerClass(Problem2_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // input and output
        FileInputFormat.addInputPath(job, new Path("/home/meta/software/IdeaProjects/sh/src/main/java/mapreduce_test/data/Vendors.csv"));
        FileInputFormat.addInputPath(job, new Path("/home/meta/software/IdeaProjects/sh/src/main/java/mapreduce_test/data/Products.csv"));
        FileOutputFormat.setOutputPath(job, new Path("./output/mapreduceOutput"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Problem2(), args));
    }
}

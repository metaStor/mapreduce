package counter;

/*
 * Create By meta On 19-1-7 下午10:31
 *
 * 统计一场异常数据的个数
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Counter_Main {

    private void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = "./src/main/java/counter/counters.txt";
        String output = "./output/countersOutput";
        Configuration configuration = new Configuration();
        String[] othersArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (!local && othersArgs.length < 2) {
            System.err.println("Usage: <Input> <Output>");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "Counters");
        job.setJarByClass(Counter_Main.class);
        job.setMapperClass(Counter_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(local ? input : othersArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(local ? output : othersArgs[1]));
        job.waitForCompletion(true);
        // 获取计数值
        Counters counters = job.getCounters();
        Counter counter1 = counters.findCounter("ErrorAccount", "TooLong");
        Counter counter2 = counters.findCounter("ErrorAccount", "TooShort");
        System.out.println(String.format("ErrorAccount: <TooLong> %s", counter1.getValue()));
        System.out.println(String.format("ErrorAccount: <TooShort> %s", counter2.getValue()));
        // 获取所有计数器
        Counters counterGroups = job.getCounters();
        for (CounterGroup group : counterGroups) {
            for (Counter counter : group) {
                System.out.println(String.format("DisplayName: '%s', Name: '%s', Value: '%s'",
                        counter.getDisplayName(), counter.getName(), counter.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Counter_Main().run(true, args);
    }
}

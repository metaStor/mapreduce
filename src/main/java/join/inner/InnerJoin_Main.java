package join.inner;

import join.Join_Comparator;
import join.Join_Partitioner;
import join.semi.SemiJoin_Main;
import join.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/*
 * Create By meta On 19-1-8 下午8:10
 */

public class InnerJoin_Main {

    public void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();
        if (args.length < 3 && !local) {
            System.err.println("Usage: <Input1> <Input2> <Output>");
            System.exit(2);
        }
        String input1 = local ? "./src/main/java/join/inner/data.txt" : otherArgs[0];
        String input2 = local ? "./src/main/java/join/inner/info.txt" : otherArgs[1];
        String output = local ? "./output/InnerJoinOutput" : otherArgs[2];
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 删除存在的输出路径
        if (fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        Job job = Job.getInstance(configuration, "InnerJoin_Main");
        // 设置运行的job
        job.setJarByClass(SemiJoin_Main.class);
        // Mapper相关设置
        job.setMapperClass(InnerJoin_Mapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(TextPair.class);
        // 设置partition
        job.setPartitionerClass(Join_Partitioner.class);
        // partition后按照条件分组
        job.setGroupingComparatorClass(Join_Comparator.class);
        // Reducer相关设置
        job.setReducerClass(InnerJoin_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入输出目录
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 内连接
        new InnerJoin_Main().run(true, args);
    }
}

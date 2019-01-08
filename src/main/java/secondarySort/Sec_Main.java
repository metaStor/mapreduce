package secondarySort;

/*
 * Create By meta On 19-1-7 下午4:27
 */

import join.semi.SemiJoin_Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Sec_Main {

    public void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();
        if (args.length < 2 && !local) {
            System.err.println("Usage: <Input> <Output> (<NumReduceTasks>)");
            System.exit(2);
        }
        int numReduceTasks = (args.length >= 3 && !local) ? Integer.parseInt(args[2]) : 1;
        String input = local ? "./src/main/java/secondarySort/secsortdata.txt" : otherArgs[0];
        String output = local ? "./output/secOutput" : otherArgs[1];
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 删除存在的输出路径
        if (fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        Job job = Job.getInstance(configuration, "SecondarySort");
        // 设置运行的job
        job.setJarByClass(SemiJoin_Main.class);
        // Mapper相关设置
        job.setMapperClass(Sec_Mapper.class);
        //当Mapper中的输出的key和value的类型和Reduce输出的key和value的类型相同时,以下两句可以省略
        job.setMapOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置partition
        job.setPartitionerClass(Sec_Partitioner.class);
        // partition后按照条件排序
        job.setSortComparatorClass(Sec_Comparator.class);
        // Reducer相关设置
        job.setReducerClass(Sec_Reducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(numReduceTasks);
        // 设置输入输出目录
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Sec_Main().run(true, args);
    }
}


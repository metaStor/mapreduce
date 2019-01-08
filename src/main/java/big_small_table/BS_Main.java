package big_small_table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/*
 * Create By meta On 19-1-7 下午9:28
 *
 * Semo-InnerJoin_Main 半连接
 *
 * 有一个大为 100G 的大表 big.txt 和一个大小为 1M 的小表 small.txt,
 * 判断小表中单词在大表中出现次数。
 * 加载小表，扫描大表
 */
public class BS_Main {


    private void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3 && !local) {
            System.err.println("Usage: <BigFile> <SmallFile> <Output>");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();
        String input1 = local ? "./src/main/java/big_small_table/big.txt" : otherArgs[0];
        String input2 = local ? "./src/main/java/big_small_table/small.txt" : otherArgs[1];
        String output = local ? "./output/bsOutput" : otherArgs[2];
        // 把小表添加到共享Cache里
        configuration.set("smallFilePath", input2);
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 删除存在的输出路径
        if (fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        Job job = Job.getInstance(configuration, "big_small_table");
        // 设置运行的job
        job.setJarByClass(BS_Main.class);
        // Mapper相关设置
        job.setMapperClass(BS_Mapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        // Reducer相关设置
        job.setReducerClass(BS_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入输出目录
        FileInputFormat.addInputPath(job, new Path(input1));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new BS_Main().run(true, args);
    }
}

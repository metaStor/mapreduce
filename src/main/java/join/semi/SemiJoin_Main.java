package join.semi;

/*
 * Created by meta on 19-1-6 下午10:30
 *
 * 需要连接两个很大的数据集，例如，user-logs和users的数据。
 * 任何一个数据集都不是足够小到可以缓存在map作业的内存中。
 * 这样看来，似乎就不能使用reduce端的连接了。尽管不是必须，可以思考以下问题：
 * 如果在数据集的连接操作中，一个数据集中有的记录由于因为无法连接到另一个数据集的记录，将会被移除。
 * 这样还需要将整个数据集放到内存中吗？
 * 在这个例子中，在user-logs用户日志中的用户仅仅是users用户数据中的用户中的很小的一部分。
 * 那么就可以从users用户数据中只取出存在于用户日志中的那部分用户的用户数据。
 * 然后就可以得到足够小到可以放在内存中的数据集。这种的解决方案就叫做半连接。
 */

import join.Join_Comparator;
import join.Join_Partitioner;
import join.TextPair;
import join.inner.InnerJoin_Mapper;
import join.inner.InnerJoin_Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SemiJoin_Main {

    public void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String[] otherArgs = genericOptionsParser.getRemainingArgs();
        if (args.length < 3 && !local) {
            System.err.println("Usage: <Users> <User-logs> <Output>");
            System.exit(2);
        }
        String input1 = local ? "./src/main/java/join/semi/users.txt" : otherArgs[0];
        String input2 = local ? "./src/main/java/join/semi/user-logs.txt" : otherArgs[1];
        String output = local ? "./output/SemiJoinOutput" : otherArgs[2];
        // users加入缓存
        configuration.set("usersFilePath", input1);
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 删除存在的输出路径
        if (fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        Job job = Job.getInstance(configuration, SemiJoin_Main.class.getName());
        // 设置运行的job
        job.setJarByClass(SemiJoin_Main.class);
        // Mapper相关设置
        job.setMapperClass(SemiJoin_Mapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        // 设置partition
        job.setPartitionerClass(Join_Partitioner.class);
        // partition后按照条件分组
        job.setGroupingComparatorClass(Join_Comparator.class);
        // Reducer相关设置
        job.setReducerClass(SemiJoin_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入输出目录
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 半连接
        new SemiJoin_Main().run(true, args);
    }
}

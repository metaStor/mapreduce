package join.semi;

import join.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/*
 * Create By meta On 19-1-8 下午8:25
 */
public class SemiJoin_Mapper extends Mapper<LongWritable, Text, TextPair, Text> {

    private Set<String> users = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        Path userFile = new Path(configuration.get("usersFilePath"));
        FileSystem fileSystem = userFile.getFileSystem(configuration);
        FSDataInputStream fsDataInputStream = fileSystem.open(userFile);
        LineReader lineReader = new LineReader(fsDataInputStream);
        Text line = new Text();
        while (lineReader.readLine(line) > 0) {
            String user = line.toString().split("\t")[0];
            users.add(user);
        }
        lineReader.close();
        fsDataInputStream.close();
        fileSystem.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件名
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] values = value.toString().split("\t");
        String user = values[0];
        if (fileName.endsWith("users.txt")) {
            // 规范数据
            if (users.contains(user) && values.length >= 3) {
                // key
                TextPair textPair = new TextPair(user, "1");  // users.txt的内容标记为1
                // 输出mapper阶段输出的数据
                context.write(textPair, new Text(values[1] + '\t' + values[2]));
            }
        } else if (users.contains(user) && fileName.endsWith("user-logs.txt")) {
            // 规范数据
            if (values.length >= 3) {
                // key
                TextPair textPair = new TextPair(user, "2");  // user-logs.txt的内容标记为2
                // 输出mapper阶段输出的数据
                context.write(textPair, new Text(values[1] + '\t' + values[2]));
            }
        }
    }

}

package big_small_table;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/*
 * Create By meta On 19-1-7 下午9:33
 */
public class BS_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static Set<String> smallTable = null;

    @Override
    protected void setup(Context context) throws IOException {
//        super.setup(context);
        smallTable = new HashSet<>();
        Path smallFilePath = new Path(context.getConfiguration().get("smallFilePath"));
        FileSystem hdfs = smallFilePath.getFileSystem(context.getConfiguration());
        FSDataInputStream hdfsReader = hdfs.open(smallFilePath);
        Text line = new Text();
        LineReader lineReader = new LineReader(hdfsReader);
        while (lineReader.readLine(line) > 0) {
            String[] values = line.toString().split(" ");
            for (String value : values) {
                if (!smallTable.contains(value)) {
                    smallTable.add(value);
                    System.out.println(value);
                }
            }
        }
        lineReader.close();
        hdfsReader.close();
        hdfs.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String[] values = value.toString().split(" ");
        for (String v : values) {
            if (smallTable.contains(v)) {
                context.write(new Text(v), one);
            }
        }
    }
}

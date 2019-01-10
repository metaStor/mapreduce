package sum_avg_group.p1;

/*
 * Created by meta on 19-1-10 下午8:33
 *
 */

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
import sum_avg_group.Department;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class p1_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private Set<Department> cacheFile = null;

    @Override
    protected void setup(Context context) throws IOException {
        cacheFile = new HashSet<>();
        Path FilePath = new Path(context.getConfiguration().get("cacheFile"));
        FileSystem hdfs = FileSystem.newInstance(context.getConfiguration());
        FSDataInputStream hdfsReader = hdfs.open(FilePath);
        LineReader lineReader = new LineReader(hdfsReader);
        Text line = new Text();
        while (lineReader.readLine(line) > 0) {
            String[] values = line.toString().split(",");
            String id = values[0].trim();
            String dept = values[1].trim();
            Department department = new Department(id, dept);
            cacheFile.add(department);
        }
        lineReader.close();
        hdfsReader.close();
        hdfs.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        // 规范数据
        if (values.length != 7) {
            return;
        }
        boolean isContain = false;
        String id = values[6].trim();
        Department outputDept = null;
        // 循坏判断自定义对象
        for (Department department : cacheFile) {
            if (department.getId().equals(id)) {
                isContain = true;
                outputDept = department;
            }
        }
        if (isContain) {
            float salary = Float.parseFloat(values[5].trim());
            // key为部门名称, value为salary
            context.write(new Text(outputDept.getDept()), new FloatWritable(salary));
        }
    }
}

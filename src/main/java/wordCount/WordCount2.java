package wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * Author: write by shenhao
 * Date: 2018/11/19
 *
 * */

public class WordCount2 {

    /*
     * <dog, t1.txt>
     * <mouse, t1.txt>
     * ...
     * <people, t2.txt>
     * <mouse, t2.txt>
     * ...
     * */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text text = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            FileSplit sp = (FileSplit) context.getInputSplit();
            String file_name = sp.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString(), " ,.?!\n\t\r\f", false);
            while (itr.hasMoreTokens()) {
                text.set(file_name);
                word.set(itr.nextToken());
                context.write(word, text);
            }
        }
    }

    /*
     * <dog, t1.txt>
     * <people, t2.txt>
     * <mouse, t1.txt, t2.txt>
     * ...
     * */
    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String sum = "";
            for (Text val : values) {
                sum += val.toString() + " ";
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public void run(boolean local, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3 && !local) {
            System.err.println("Usage: <Input1> <Input2> <Output>");
            System.exit(2);
        }
        String input1 = local ? "./src/main/java/wordCount/t1.txt" : args[0];
        String input2 = local ? "./src/main/java/wordCount/t2.txt" : args[1];
        String output = local ? "./output/wordCountOutput" : args[2];
        Configuration conf = new Configuration();
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        // 删除存在的输出路径
        if (fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        Job job = Job.getInstance(conf, WordCount2.class.getName());
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        new WordCount2().run(true, args);
    }
}

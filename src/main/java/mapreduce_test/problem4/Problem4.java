package mapreduce_test.problem4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Problem4 extends Configured implements Tool {

    public static class P2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private static String fileName;

        @Override
        protected void setup(Context context) {
            // 此方法只运行一次
            // 得到输入的文件名
            fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        /*
         * <订单编号, 标识+内容>
         *  ...
         * */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if (fileName.endsWith("Orders.csv")) {
                String clientId = values[2];  // 客户id
                if (clientId.equals("1000000001")) { // 此处筛选客户id
                    String orderId = values[0];  // 订单id
                    context.write(new Text(orderId), new Text("1" + orderId));  // Orders.csv标记为1
                }

            } else if (fileName.endsWith("OrderItems.csv")) {
                String orderId = values[0]; // 订单id
                String count = values[3]; // 订单数量
                String price = values[4]; // 订单价格
                float priceSum = Float.parseFloat(price) * Float.parseFloat(count);  // 总价
                context.write(new Text(orderId), new Text("2" + String.valueOf(priceSum)));  // OrderItems.csv标记为2
            }
        }
    }

    public static class P2_Reducer extends Reducer<Text, Text, Text, FloatWritable> {

        private List<String> clientId = new ArrayList<>();  // 存放Orders.csv的订单id
        private List<String> Price = new ArrayList<>();  // 存放OrderItems.csv的订单id以及价格

        /*
         * <订单编号, 处理后的内容>
         * ...
         * */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 利用在map端的标记将两表的源数据分开
            for (Text value : values) {
                String info = value.toString();
                if (info.startsWith("1")) {
                    clientId.add(info.substring(1));
                } else if (info.startsWith("2")) {
                    Price.add(key.toString() + info.substring(1));  // 记录订单id和订单价格
                }
            }
        }

        // 利用cleanup()方法最后统计总和
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String id : clientId) {
                float sum = 0;
                for (String price : Price) {
                    float p = Float.parseFloat(price.substring(5));  // 取价格
                    String id1 = price.substring(0, 5); // 取id
                    // 进行关联，相等就相加
                    if (id1.equals(id)) {
                        sum += p;
                    }
                }
                // 输出
                context.write(new Text(id), new FloatWritable(sum));
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Jar class
        job.setJarByClass(Problem4.class);
        // Mapper
        job.setMapperClass(P2_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Reducer
        job.setReducerClass(P2_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        // input and output
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Problem4(), args));
    }
}

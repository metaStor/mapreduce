package mapreduce_test.problem2;

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

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem2_Mapper extends Mapper<LongWritable, Text, Text, ProductText> {

    private String fileName = null;
    private Set<String> cacheFile = null;

    private void getFileName(Context context) {
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    private void cache(Context context) throws IOException {
        cacheFile = new HashSet<>();
        Path FilePath = new Path(context.getConfiguration().get("cacheFile"));
        FileSystem hdfs = FileSystem.newInstance(context.getConfiguration());
        FSDataInputStream hdfsReader = hdfs.open(FilePath);
        Text line = new Text();
        LineReader lineReader = new LineReader(hdfsReader);
        while (lineReader.readLine(line) > 0) {
            String[] values = line.toString().split(",");
            String country = values[6].trim();
            String vendId = values[0].trim();
            if (country.equals("USA")) {
                cacheFile.add(vendId);
            }
        }
        lineReader.close();
        hdfsReader.close();
        hdfs.close();
    }

    private void relationMethod(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        ProductText product = null;
        if (fileName.endsWith("Vendors.csv")) {
            // right data
            if (values.length == 7) {
                String country = values[6].trim();
                if (country.equals("USA")) {
                    String vendId = values[0];
                    product = new ProductText(vendId, "None", 0, "");
                    context.write(new Text(vendId), product);
                }
            }
        } else if (fileName.endsWith("Products.csv")) {
            // right data
            if (values.length == 5) {
                float price = Float.parseFloat(values[3].trim());
                if (price > 5) {
                    String vendId = values[1];
                    String name = values[2];
                    String describute = values[4];
                    product = new ProductText(vendId, name, price, describute);
                    context.write(new Text(vendId), product);
                }
            }
        }
    }

    private void cacheMethod(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        if (values.length == 5 && cacheFile.contains(values[1].trim())) {
            String vendId = values[1];
            String name = values[2];
            float price = Float.parseFloat(values[3].trim());
            if (price > 5) {
                String describute = values[4];
                ProductText product = new ProductText(vendId, name, price, describute);
                context.write(new Text(vendId), product);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        getFileName(context);
//        cache(context);
    }

    /*
     * <vend_id, product_object1>
     * <vend_id, product_object2>
     *  ...
     * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        relationMethod(key, value, context);
        relationMethod(key, value, context);
    }
}

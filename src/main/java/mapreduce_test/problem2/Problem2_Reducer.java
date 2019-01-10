package mapreduce_test.problem2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ud02 on 1/9/19.
 */
public class Problem2_Reducer extends Reducer<Text, ProductText, Text, Text> {

    private void relationMethod(Text key, Iterable<ProductText> values, Context context) throws IOException, InterruptedException {
        List<Text> vendors = new ArrayList<>();
        List<Text> products = new ArrayList<>();
        /*
         * 此处遇到对象覆盖问题，原因是下列for循环中的对象用的都是同一个地址
         * 采用Text格式的ArrayList解决
         */
        for (ProductText product : values) {
            if (product.getName().toString().trim().equals("None")) {
                vendors.add(product.getId());
//                System.out.println("key1: " + key.toString() + "\tkey2: " + product.getId().toString());
            } else {
                products.add(new Text(product.toString()));
//                System.out.println("key1: " + key.toString() + "\tkey2: " + product.getId().toString() + "\tvalue: " + product.toString());
            }
        }
        // 交集
        for (Text vendorId : vendors) {
            for (Text product : products) {
                context.write(vendorId, product);
            }
        }
    }

    private void cacheMethod(Text key, Iterable<ProductText> values, Context context) throws IOException, InterruptedException {
        while (values.iterator().hasNext()) {
            context.write(key, new Text(values.iterator().next().toString()));
        }
    }


    @Override
    protected void reduce(Text key, Iterable<ProductText> values, Context context) throws IOException, InterruptedException {
        relationMethod(key, values, context);
//        cacheMethod(key, values, context);
    }
}

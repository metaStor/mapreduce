package secondarySort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Create By meta On 19-1-7 下午4:42
 */
public class Sec_Reducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {

    @Override
    protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        context.write(key, NullWritable.get());
    }
}

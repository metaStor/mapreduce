package secondarySort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Create By meta On 19-1-7 下午8:40
 */
public class Sec_Partitioner extends Partitioner<IntPair, NullWritable> {

    @Override
    public int getPartition(IntPair intPair, NullWritable nullWritable, int i) {
        return Math.abs(intPair.getFirst().get()) % i;
    }
}

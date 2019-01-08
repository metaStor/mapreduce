package join;

/*
 * Created by meta on 19-1-6 下午11:32
 *
 * 分组, 更好的将map任务的结果均匀分配给reducer
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Join_Partitioner extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(TextPair textPair, Text text, int numReduceTasks) {
        return Math.abs(textPair.getFirst().hashCode() & 127) % numReduceTasks;
    }
}

package sum_avg_group.p6;

/*
 * Created by meta on 19-1-10 下午8:34
 *
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class p6_Reducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    // 所有员工表
    private List<String> employeesList = null;
    // 员工对应的上司表
    private Map<String, String> employeeToManager = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        employeesList = new ArrayList<>();
        employeeToManager = new HashMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
         * 7934@7782
         * 7844@7698
         * 7839@7839 (root)
         * ....
         * */
        // record to tables
        for (Text text : values) {
            String emp = text.toString().split("@")[0].trim();
            String mag = text.toString().split("@")[1].trim();
            employeesList.add(emp);
            // 员工id(unique)：上司id
            employeeToManager.put(emp, mag);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int len = employeesList.size();
        for (int i = 0; i < len - 1; ++i) {
            for (int j = i + 1; j < len; ++j) {
                String src = employeesList.get(i);
                String tar = employeesList.get(j);
                int distance = calculateDistance(src, tar);
                context.write(NullWritable.get(),
                        new Text("The Distance Between " + src + " and " + tar + " is " + distance));
            }
        }
    }

    private int calculateDistance(String A, String B) {
        // self
        if (A.equals(B)) {
            return 0;
        }
        // A是B的boss or B是A的boss
        else if (employeeToManager.get(A).equals(B) || employeeToManager.get(B).equals(A)) {
            return 0;
        }
        // A,B同boss
        else if (employeeToManager.get(A).equals(employeeToManager.get(B))) {
            return 1;
        }
        // 其他情况
        else {
            // A可接触到的所有员工集合
            List<String> A_Table = new ArrayList<>();
            // B可接触到的所有员工集合
            List<String> B_Table = new ArrayList<>();
            // start collecting ...
            A_Table.add(A);
            String cur_A = A;
            while (!employeeToManager.get(cur_A).equals(cur_A)) {
                cur_A = employeeToManager.get(cur_A);
                A_Table.add(cur_A);
            }
            B_Table.add(B);
            String cur_B = B;
            while (!employeeToManager.get(cur_B).equals(cur_B)) {
                cur_B = employeeToManager.get(cur_B);
                B_Table.add(cur_B);
            }
            // collecting end
            // 寻找汇合点
            int di = 0, dj = 0;
            boolean getIt = false;
            for (; di < A_Table.size(); ++di) {
                String a = A_Table.get(di);
                for (; dj < B_Table.size(); ++dj) {
                    if (a.equals(B_Table.get(dj))) {
                        getIt = true;
                        break;
                    }
                }
                if (getIt) break;
            }
            return di + dj;
        }
    }
}

package sum_avg_group.p5;

/*
 * Created by meta on 19-1-10 下午8:34
 *
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class p5_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
         * shuffle后得到的：
         * key: 7839	M CLARK 2450
         * key: 7839	M JONES 2975
         * key: 7839	E KING 5000
         * key: 7839	M BLAKE 2850
         * 汇总了员工号为7839的的数据集合
         * E 标志就是该员工自己的薪水
         * M 是下属及其薪水
         * */
        // 下属集合
        List<String> employees = new ArrayList<>();
        // 经理工资
        float ManagerSalary = -1f;
        for (Text value : values) {
            String str = value.toString();
//            System.out.println("key: " + key.toString() + "\tvalue: " + str);
            // have boss
            if (str.startsWith("M")) {
                employees.add(str.substring(1).replace("#", "\t"));
            }
            // it's a boss
            else if (str.startsWith("E")) {
                ManagerSalary = Float.parseFloat(str.substring(1).split("#")[1]);
            }
        }
        // 比较经理和下属的工资
        for (String emp : employees) {
            if (Float.parseFloat(emp.split("\t")[1]) >= ManagerSalary) {
                context.write(new Text(emp.split("\t")[0]), new Text(emp.split("\t")[1]));
            }
        }
    }
}

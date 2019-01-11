package sum_avg_group.p5;

/*
 * Created by meta on 19-1-10 下午8:33
 *
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class p5_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        // 规范数据
        if (values.length != 7) {
            return;
        }
        String id = values[0].trim();
        String name = values[1].trim();
        String salary = values[5].trim();
        String superiorId = values[3].trim();  // it might is "".
        if (!superiorId.equals("")) {  // Employee
            /*
             * 例如读取： 7698,BLAKE,7839,2850
             * 输出： E 7698 BLAKE 2850 (员工表：E 员工id 员工姓名 员工薪水)
             *       M 7839 BLAKE 2850 (经理表：M 经理id 员工姓名 员工薪水)
             * */
            context.write(new Text(id), new Text("E" + name + "#" + salary));
            context.write(new Text(superiorId), new Text("M" + name + "#" + salary));
        } else {  // Manager
            /*
             * 例如读取： 7839,KING,,5000
             * 输出： E 7839 KING 5000 (员工表：E 员工id 员工姓名 员工薪水)
             *       (无经理，此为大boss)
             * */
            context.write(new Text(id), new Text("E" + name + "#" + salary));
        }
    }
}

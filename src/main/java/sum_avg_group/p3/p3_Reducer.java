package sum_avg_group.p3;

/*
 * Created by meta on 19-1-10 下午10:40
 *
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class p3_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int date = Integer.MAX_VALUE;
        String name = null;
        for (Text value : values) {
            String n = value.toString().split("#")[0];
            // 分割出年月日
            String[] d = value.toString().split("#")[1].split("-");
            String month = d[1].substring(0, d[1].length() - 2);
            String day = d[2];
            if (month.length() == 1) {
                month = "0" + month;
            }
            if (day.length() == 1) {
                day = "0" + day;
            }
            int d1 = Integer.parseInt(day + month + d[0]);
            if (d1 < date) {
                name = n;
                date = d1;
            }
        }
        // 按照 - 分割
        String dateStr = String.valueOf(date);
        String[] strings = new String[dateStr.length() / 2];
        int t = 0;
        for (int i = 0; i < dateStr.length() / 2; i++) {
            strings[i] = dateStr.substring(t, t + 2);
            t += 2;
        }
        context.write(key, new Text("Elder numbers: " + name + "\tDate: " + String.join("-", strings)));
    }
}

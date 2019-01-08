package join.left_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class LeftJoin_Mapper extends Mapper<Object, Text, Text, Text> {

    private HashMap<String, List<String>> nameToUser;

    private HashMap<String, List<String>> loadUsers(String pathString)
            throws IOException {
        HashMap<String, List<String>> users_name = new HashMap<>();
        Charset charset = Charset.forName("utf-8");
        Path path = Paths.get(pathString);
        try (BufferedReader reader = Files.newBufferedReader(path, charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] sp = line.split("\t");
                String username = sp[0];
                users_name.put(username, Arrays.asList(sp));
            }
        }
        return users_name;
    }

    @Override
    protected void setup(Context context)
            throws IOException {
        // 放入自己项目下的users.txt的绝对路径
        nameToUser = loadUsers("./src/main/java/join.left_join/users.txt");
    }

    @Override
    // keyIn, valueIn, Context<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] logs = value.toString().split("\t");
        String username = logs[0];
        // 在整个用户表查找
        List<String> user = nameToUser.get(username);
        String userStr;
        // 关联log记录和jim用户记录
        userStr = (user != null) ? String.join(",", user) : "";
        String logStr = String.join(",", Arrays.asList(logs));
        String outValue = (user != null) ? (logStr + "|" + userStr) : (logStr);
        context.write(new Text(username), new Text(outValue));
    }
}

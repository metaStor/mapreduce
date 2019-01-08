package simplemr;

//import com.sun.xml.internal.ws.api.ha.StickyFeature;

import java.awt.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;

/**
 * 单机简化版MapReduce运行框架
 */
public class SimpleMapReduce {

    /**
     * MapReduce的shuffle阶段。
     *
     * @param outMap map任务的输出。
     * @return shuffle阶段的输出，即reduce任务的输入。注意key是有序的。
     */
    private static SortedMap<String, List<String>> shuffle(List<Pair> outMap) {
        SortedMap<String, List<String>> result = new TreeMap<>();
        // TODO 补充完成该方法，实现简单的shuffle过程
        for (Pair p :
                outMap) {
            if (!result.containsKey(p.key)) {
                List<String> values = new ArrayList<>();
                values.add(p.value);
                result.put(p.key, values);
            } else {
                result.get(p.key).add(p.value);
            }
        }
        return result;
    }

    /**
     * 运行一个MapReduce作业。
     *
     * @param inPath  输入文件路径
     * @param outPath 输出文件路径
     * @param mapTask map任务
     * @param reduceTask reduce任务
     */
    public static void run(String inPath,
                           String outPath,
                           MapTask mapTask,
                           ReduceTask reduceTask) {
        Path in = Paths.get(inPath);
        Path out = Paths.get(outPath);
        Charset charset = Charset.forName("UTF-8");
        List<Pair> outMap = new ArrayList<>(); // 存放map任务的全部输出
        try (BufferedReader reader = Files.newBufferedReader(in, charset)) {
            /*** map任务阶段 ***/
            String line = null;
            int numLine = 0;
            while ((line = reader.readLine()) != null) { // 读入一行
                numLine++;
                String k = Integer.toString(numLine);
                List<Pair> list = mapTask.map(k, line);
                outMap.addAll(list);
            }
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
            System.exit(1);
        }
        /*** shuffle阶段 ***/
        Map<String, List<String>> keyToList = shuffle(outMap);
        /*** reduce任务阶段 ***/
        try (BufferedWriter writer = Files.newBufferedWriter(out, charset)) {
            for (Map.Entry<String, List<String>> entry : keyToList.entrySet()) {
                Pair p = reduceTask.reduce(entry.getKey(), entry.getValue());
                if (p != null) {
                    writer.write(p.key);
                    writer.write(',');
                    writer.write(p.value);
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
            System.exit(1);
        }
    }
}

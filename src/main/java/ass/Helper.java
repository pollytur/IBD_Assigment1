package ass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Helper {


    public static List<Path> getPathsByName(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(path), false);
        List<Path> pathArr = new ArrayList<Path>();
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            Path loc = fileStatus.getPath();
            if (loc.getName().startsWith("part-r-")) {
                pathArr.add(loc);
            }
        }

        return pathArr;
    }

    public static  void readFile(FileSystem fs, Path path, Configuration conf) throws IOException {

        byte[] bytes = IOUtils.readFullyToByteArray(fs.open(path));
        String content = new String(bytes, StandardCharsets.UTF_8);

        for (String line : content.split("\n")) {
            String[] parts = line.split("\t");
            conf.set(parts[0], parts[1]);
        }
    }


    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse(line);
//                https://stackoverflow.com/questions/18830813/how-can-i-remove-punctuation-from-input-text-in-java
//                removes all non-letter characters
                String[] words = json.get("text").toString().toLowerCase()
                        .replaceAll("[^a-zA-Z ]", " ").
                        trim().split("\\s+");
                for (String w : words) {
                    context.write(new Text(w), one);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class IntSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, one);
        }
    }

    public static double getNumberOfFiles(Configuration conf, String folder) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fsIterator = fs.listFiles(new Path(folder), false);
        double counter = 0.0;
        while (fsIterator.hasNext()) {
            counter += 1;
            fsIterator.next();
        }

        return counter;
    }

}

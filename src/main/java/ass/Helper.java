package ass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class Helper {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();

            try {
                JSONObject json = (JSONObject) parser.parse(line);
//                https://stackoverflow.com/questions/18830813/how-can-i-remove-punctuation-from-input-text-in-java
//                removes all non-letter characters
                String[] words = json.get("text").toString().replaceAll("[^a-zA-Z ]", " ").
                        trim().split("\\s+");
                for (int i = 0; i < words.length; i++) {
                    context.write(new Text(words[i]), one);
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

}

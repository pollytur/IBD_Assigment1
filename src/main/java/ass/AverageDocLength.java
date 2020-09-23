package ass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

import static ass.Helper.getNumberOfFiles;

public class AverageDocLength {

    public static class CountDocLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();

            try {
                JSONObject json = (JSONObject) parser.parse(line);

                String[] words = json.get("text").toString().replaceAll("[^a-zA-Z ]", " ").
                        trim().split("\\s+");
                context.write(one, new IntWritable(words.length));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }


    public static class CountDocLengthReducer extends Reducer<IntWritable,IntWritable,Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Configuration conf = context.getConfiguration();
            context.write(new Text("avgdl"), new DoubleWritable(sum*1.0/Double.parseDouble(conf.get("NFILES"))));
        }
    }



    public static int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        double numberOfFiles = getNumberOfFiles(conf, args[0]);
        conf.set("NFILES", numberOfFiles + "");
        Job job = Job.getInstance(conf, "Average Document Length");
        job.setJarByClass(AverageDocLength.class);
        job.setMapperClass(CountDocLengthMapper.class);
        job.setReducerClass(CountDocLengthReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }


}

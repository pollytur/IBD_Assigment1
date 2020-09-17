package ass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.Math;

import java.io.IOException;

// Document Count = TokenizerMapper + IntSumCombiner + IntSumReducer

public class DocumentCount extends Helper {



    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Configuration conf = context.getConfiguration();
            context.write(key, new DoubleWritable(Math.log10(1.0+Double.parseDouble(conf.get("NFILES"))/sum)));
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

    public static int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        double numberOfFiles = getNumberOfFiles(conf, args[0]);
        conf.set("NFILES", numberOfFiles + "");

        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCount.class);
        job.setMapperClass(Helper.TokenizerMapper.class);
        job.setCombinerClass(Helper.IntSumCombiner.class);
        job.setReducerClass(DocumentCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

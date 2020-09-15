package ass;

// Word Enumerator = TokenizerMapper + IntSumCombiner + WordEnumeratorReduce

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordEnumerator extends Helper {

    public static class WordEnumeratorReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            Long id = context.getCounter("enumerate", "count").getValue();
            context.getCounter("enumerate", "count").increment(1);
            context.write(key, new IntWritable(id.intValue()));

        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word enumerator");
        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(Helper.TokenizerMapper.class);
        job.setCombinerClass(Helper.IntSumCombiner.class);
        job.setReducerClass(WordEnumerator.WordEnumeratorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

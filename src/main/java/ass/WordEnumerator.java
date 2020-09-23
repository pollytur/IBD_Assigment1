package ass;

// Word Enumerator = TokenizerMapper + IntSumCombiner + WordEnumeratorReduce

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class WordEnumerator extends Helper {

    public static class WordEnumeratorReducer extends Reducer<Text, IntWritable, Text, Text> {
        enum WordEnum {
            ID
        }


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Counter counter = context.getCounter(WordEnum.class.getName(), WordEnum.ID.toString());
            long id = counter.getValue();
            counter.increment(1);
            context.write(key, new Text(id+"_"+context.getConfiguration().get(key.toString())));
        }



        @Override
        protected void setup(Reducer.Context context) throws IOException {
            try {
                Configuration conf = context.getConfiguration();
                FileSystem fs = FileSystem.get(conf);
                // the second boolean parameter here sets the recursion to true
                RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(context.getConfiguration().get("_path2")), false);
                while (fileStatusListIterator.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatusListIterator.next();
                    Path loc = fileStatus.getPath();
                    if (loc.getName().startsWith("part-r-")) {
                        readFile(fs, loc, context.getConfiguration());
                    }
                }

            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        }
    }

    public static int run(String[] args) throws Exception {
        String[] argsCleared = new String[]{args[0], args[2]};
        int r2 = DocumentCount.run(argsCleared);


        Configuration conf = new Configuration();
        conf.set("_path2", args[2]);
        Job job = Job.getInstance(conf, "word enumerator");


        List<Path> cacheFiles2 = getPathsByName(conf, args[2]);
        for (Path p : cacheFiles2)
            job.addCacheFile(p.toUri());


        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(Helper.TokenizerMapper.class);
        job.setCombinerClass(Helper.IntSumCombiner.class);
        job.setReducerClass(WordEnumerator.WordEnumeratorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}

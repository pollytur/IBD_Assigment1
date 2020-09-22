package ass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RelevanceAnalizator {


    // Mapper in MapReduce paradigm
    public static class AnalizatorMapper extends Mapper<Object, Text, IntWritable, Text> {

    }

    // Reducer in MapReduce paradigm
    public static class AnalizatorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    }

    private static void run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("_path", args[1]);
        Job job = Job.getInstance(conf, "Query execution");

        QueryVectorizer vectorizer = new QueryVectorizer(conf, args[2]);
        // List<Path> cacheFiles = Helper.getPathsByName(conf, args[1]);
        // for (Path p : cacheFiles)
        //     job.addCacheFile(p.toUri());

        // job.setJarByClass(Indexer.class);
        // job.setMapperClass(AnalizatorMapper.class);
        // job.setReducerClass(AnalizatorReducer.class);
        // job.setOutputKeyClass(IntWritable.class);
        // job.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        //        0 argument is a query
        //        1 argument is output for analizator
        //        2 argument is a path to word ID and IDFs
        run(args);
    }
}

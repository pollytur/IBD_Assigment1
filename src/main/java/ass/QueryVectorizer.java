package ass;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import static ass.Indexer.inside_reduce;
import static ass.Indexer.setupPlaceholder;

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

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class QueryVectorizer {

//https://github.com/kckusal/F19-IU-Big-Data-Assignment-1
//Inspired by  https://github.com/gauravsinghaec/HADOOP-DISTRIBUTED-CACHE

// Document Count = TokenizerMapper + IntSumCombiner + IntSumReducer
// Word Enumerator = TokenizerMapper + IntSumCombiner + WordEnumeratorReduce
// Vocabulary
// Indexer


    public static class QueryMapper extends Mapper<Object, Text, IntWritable, Text> {
        static double[] queryVector;

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            setupPlaceholder(conf);

            String[] words = conf.get("_query").replaceAll("[^a-zA-Z -]", " ").
                    trim().split("\\s+");

            String[] idsStream = Arrays.stream(words).map(conf::get).collect(Collectors.toList()).toArray(new String[0]);

            String output = inside_reduce(idsStream);

            queryVector = Arrays.stream( output.split("="))
                    .mapToDouble(Double::parseDouble)
                    .toArray();


        }



    }

    private static void run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("_query", args[2]);
        conf.set("_path", args[0]);
        Job job = Job.getInstance(conf, "SAMARAAAAAA word count");
        List<Path> cacheFiles = Helper.getPathsByName(conf, args[0]);
        for (Path p : cacheFiles)
            job.addCacheFile(p.toUri());

        job.setJarByClass(QueryVectorizer.class);
        job.setMapperClass(QueryMapper.class);
        job.setReducerClass(ass.Indexer.IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input from  Document Count output
//        1 argument is output
//        2 argument is query
        run(args);
    }


}




package ass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.ToDoubleBiFunction;
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

    /**
     * Get vector from given document
     */
    private double[] parseDoc(String document) {
        String[] temp = document.split("=");

        double[] ret = new double[temp.length];
        for (String str : temp) {
            ret[i] = Double.parseDouble(str);
        }

        return ret;
    }

    /**
     * Compute relevance score using naive approach:
     * simply find dot product between query and document
     */
    private double getRelevanceScoreBasic(double[] query, double[] document) {
        double score = 0;

        for (int i = 0; i < query.length; i++) {
            score += query[i] * document[i];
        }
        return score;
    }

    /**
     * Compute relevance score using more advanced
     * Okapi BM25 ranking function
     */
    private double getRelevanceScoreBM25(double[] query, double[] document, double docLength) {
        double score = 0;

        double b = 0.75;
        double k1 = 2;

        // In assignment, there is a mistake:
        // It states that we need IDF(di);
        // However, it should be IDF(qi)
        double idf = 1; // TODO
        double avgLength = 1; // TODO

        for (int i = 0; i < query.length; i++) {
            double tmp1 = document[i] * (k1 + 1);
            double tmp2 = document[i] + k1 * (1-b + b * docLength/avgLength);

            score += idf * tmp1 / tmp2;
        }
        return score;
    }

    /**
     * A useful function for BM25 ranker
     */
    private int getDocLength(String document) {
        String[] temp = document.split("=");

        int len = 0;
        for (String str : temp) {
            if (!str.equals("0")) {
                len++;
            }
        }
        return len;
    }

    public static void run(String[] args) throws Exception {

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
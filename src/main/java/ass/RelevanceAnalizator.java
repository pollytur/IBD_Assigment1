package ass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static ass.Indexer.*;


public class RelevanceAnalizator {
    static double[] queryVector;


    // Mapper in MapReduce paradigm
    public static class AnalizatorMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        /**
         * Compute relevance score using naive approach:
         * simply find dot product between query and document
         */
        private Double getRelevanceScoreBasic(double[] query, double[] document) {
            double score = 0;

            for (int i = 0; i < query.length; i++) {
                score += query[i] * document[i];
            }
            return score;
        }


        @Override
        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            setupPlaceholder(conf);


            String[] words = conf.get("_query").replaceAll("[^a-zA-Z -]", " ").
                    trim().split("\\s+");

            String[] idsStream = Arrays.stream(words).map(conf::get).collect(Collectors.toList()).toArray(new String[0]);

            String output = inside_reduce(idsStream);

            queryVector = Arrays.stream(output.split("="))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            double[] coefs = Arrays.stream(line[1].split("="))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
//          multiply by -1 because hadoop reduce will return the values in the ascending order
            DoubleWritable score = new DoubleWritable((-1.0) * getRelevanceScoreBasic(queryVector, coefs));

            context.write(score, new Text(line[0]));

        }

    }

    // Reducer in MapReduce paradigm
    public static class AnalizatorReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        enum MaxDocs {
            LENGTH
        }

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Counter counter = context.getCounter(MaxDocs.class.getName(), MaxDocs.LENGTH.toString());
            Long topLength = Long.parseLong(context.getConfiguration().get("_top_length"));
            if (topLength > counter.getValue()) {
                String[] prep = concatStringsWSep(values, "&").split("&");
                if (counter.getValue() + prep.length <= topLength) {
                    for (String s : prep) {
                        System.out.println(String.valueOf(key).join(" ", prep));
                        context.write(new DoubleWritable( Double.parseDouble(key.toString())*(-1.0)), new Text(s));
                    }
                    counter.increment(prep.length);
                }
                else{ 
                    Iterator<Text> iter = values.iterator();
                    for (int i=0; i<(topLength -counter.getValue()); i++){
                       Text txt = iter.next();
                        System.out.println(String.valueOf(key).join(" ", txt.toString()));
                        context.write(new DoubleWritable( Double.parseDouble(key.toString())*(-1.0)), txt);
                    }

                }

            }


        }

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
            double tmp2 = document[i] + k1 * (1 - b + b * docLength / avgLength);

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

    private static void run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("_query", args[3]);
        conf.set("_path", args[0]);
        conf.set("_top_length", args[4]);
        Job job = Job.getInstance(conf, "SAMARAAAAAA word count");
        List<Path> cacheFiles = Helper.getPathsByName(conf, args[0]);
        for (Path p : cacheFiles)
            job.addCacheFile(p.toUri());

        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(AnalizatorMapper.class);
        job.setReducerClass(ass.Indexer.IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input from  Document Count output
//        1 argument is input from  Indexer output
//        2 argument is output
//        3 argument is query
//        4 argument is length of top
        run(args);
    }

}
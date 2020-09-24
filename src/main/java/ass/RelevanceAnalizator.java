package ass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
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


            String[] words = conf.get("_query").toLowerCase().replaceAll("[^a-zA-Z -]", " ").
                    trim().split("\\s+");

            for (int i = 0; i < words.length; i++) {
                try {
                    words[i] = conf.get(words[i]);
                } catch (Exception e) {
//                    just ignore unknown words
                    e.printStackTrace();
                }

            }

//            String[] idsStream = Arrays.stream(words).collect(Collectors.toList()).toArray(new String[0]);

            String output = inside_reduce(words);

            queryVector = Arrays.stream(output.split("="))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            String[] encodings = line[1].split("___");
            double[] coefs = Arrays.stream(encodings[0].split("="))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
//          multiply by -1 because hadoop reduce will return the values in the ascending order
            DoubleWritable score = new DoubleWritable((-1.0) * getRelevanceScoreBasic(queryVector, coefs));

            context.write(score, new Text(encodings[1]));

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
                        context.write(new DoubleWritable(Double.parseDouble(key.toString()) * (-1.0)), new Text(s));
                    }
                    counter.increment(prep.length);
                } else {
                    Iterator<Text> iter = values.iterator();
                    for (int i = 0; i < (topLength - counter.getValue()); i++) {
                        Text txt = iter.next();
                        System.out.println(String.valueOf(key).join(" ", txt.toString()));
                        context.write(new DoubleWritable(Double.parseDouble(key.toString()) * (-1.0)), txt);
                    }

                }

            }

        }

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
        job.setReducerClass(AnalizatorReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        int res = (job.waitForCompletion(true) ? 0 : 1);
        try {
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[2]), false);
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                Path loc = fileStatus.getPath();
                if (loc.getName().startsWith("part-r-")) {
                    byte[] bytes = IOUtils.readFullyToByteArray(fs.open(loc));
                    String content = new String(bytes, StandardCharsets.UTF_8);
                    System.out.println(content);
                }
            }

        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("Indexer must get the following 5 arguments:");
        System.out.println("1st: Directory containing Document Count output");
        System.out.println("2nd: Directory containing Indexer output");
        System.out.println("3rd: Directory for output");
        System.out.println("4th: Query itself");
        System.out.println("5th: Number of relevant documents to output");
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input from  Word Enumerator output
//        1 argument is input from  Indexer output
//        2 argument is output
//        3 argument is query
//        4 argument is length of top
        if (args[0].equals("--help")) {
            printHelp();
        } else if (args.length != 5) {
            System.out.println("Invalid number of arguments \n");
            printHelp();
        } else {
            boolean isNumber = true;

            try {
                Integer.parseInt(args[4]);
            } catch (Exception e) {
                System.out.println("The last argument should be a number\n");
                printHelp();
                isNumber = false;
            }

            if (isNumber) {
                run(args);

            }
        }
    }

}
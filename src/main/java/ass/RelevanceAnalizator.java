package ass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

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

import static ass.Helper.readFile;
import static ass.Indexer.*;


public class RelevanceAnalizator {



    // Mapper in MapReduce paradigm
    public static class AnalizatorMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        static HashMap<Integer, Double> hm = new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            setupPlaceholder(conf);


            String[] words = conf.get("_query").toLowerCase().replaceAll("[^a-zA-Z -]", " ").
                    toLowerCase().trim().split("\\s+");

//            String[] idsStream = Arrays.stream(words).map(conf::get).collect(Collectors.toList()).toArray(new String[0]);

            for (String st : words) {
//                throw new RuntimeException(String.format("THE S is %s %s", st, conf.get(st)));
//                String[] splitted = conf.get(st).split("_");
//                hm.put(Integer.parseInt(splitted[0]), Double.parseDouble(splitted[1]));
                try {
                    String[] splitted = conf.get(st).split("_");
                    hm.put(Integer.parseInt(splitted[0]), Double.parseDouble(splitted[1]));
                } catch (Exception e) {
//                    ignore is if there is an unknown words in the query
                    e.printStackTrace();
//                    throw new RuntimeException(String.format("THE st is %s", conf.get(st)));

                }
            }

            try {
                FileSystem fs = FileSystem.get(conf);
                // the second boolean parameter here sets the recursion to true
                RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(conf.get("_avgdl")), false);
                while (fileStatusListIterator.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatusListIterator.next();
                    Path loc = fileStatus.getPath();
                    if (loc.getName().startsWith("part-r-")) {
                        readFile(fs, loc, conf);
                    }
                }
            } catch (NullPointerException e) {
                e.printStackTrace();
            }


        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            String[] elements = line[1].split("___");
            String[] coefs = elements[0].split("=");
            Double res = 0.0;
            double avgLength = Double.parseDouble(context.getConfiguration().get("avgdl"));
            Integer thisLength = Integer.parseInt(elements[2]);
            int kOne = 2;
            double b = 0.75;
            for (String s : coefs) {
//                throw new RuntimeException(String.format("THE S is %s", hm.keySet().toString()));

                String[] splitted = s.split(",");
                if (hm.keySet().contains(Integer.parseInt(splitted[0]))) {
                    int freq = Integer.parseInt(splitted[1]);
//                    BM25 calculations
                    res += hm.get(Integer.parseInt(splitted[0])) *
                            freq * (kOne + 1) / (freq - b + b * thisLength * 1.0 / avgLength);
                }

            }
//          multiply by -1 because hadoop reduce will return the values in the ascending order
            res = (-1.0) * res;

            context.write(new DoubleWritable(res), new Text(elements[1]));

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
        conf.set("_query", args[4]);
        conf.set("_path", args[0]);
        conf.set("_top_length", args[5]);
        conf.set("_avgdl", args[2]);
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
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        int res = (job.waitForCompletion(true) ? 0 : 1);
        try {
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[3]), false);
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
        System.out.println("3th: Directory containing AverageDocLength output");
        System.out.println("4rd: Directory for output");
        System.out.println("5th: Query itself");
        System.out.println("6th: Number of relevant documents to output");
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input from  Word Enumerator output
//        1 argument is input from  Indexer output
//        2 argument is input from  Average File Length output

//        3 argument is output
//        4 argument is query
//        5 argument is length of top
        if (args[0].equals("--help")) {
            printHelp();
        } else if (args.length != 6) {
            System.out.println("Invalid number of arguments \n");
            printHelp();
        } else {
            boolean isNumber = true;

            try {
                Integer.parseInt(args[5]);
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
package ass;

//https://github.com/kckusal/F19-IU-Big-Data-Assignment-1
//Inspired by  https://github.com/gauravsinghaec/HADOOP-DISTRIBUTED-CACHE

// Document Count = TokenizerMapper + IntSumCombiner + IntSumReducer
// Word Enumerator = TokenizerMapper + IntSumCombiner + WordEnumeratorReduce
// Vocabulary
// Indexer

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

import static ass.Helper.readFile;


public class Indexer {

    protected static void setupPlaceholder(Configuration conf) throws IOException {
        try {
            FileSystem fs = FileSystem.get(conf);
            // the second boolean parameter here sets the recursion to true
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(conf.get("_path")), false);
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


    public static class IndexerMapper extends Mapper<Object, Text, IntWritable, Text> {

        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            setupPlaceholder(conf);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();
            Configuration conf = context.getConfiguration();

            try {
                JSONObject json = (JSONObject) parser.parse(line);
                String[] words = json.get("text").toString().replaceAll("[^a-zA-Z ]", " ").
                        trim().split("\\s+");

                List<String> idsStream = Arrays.stream(words).map(conf::get).collect(Collectors.toList());

                for (int i=0; i<idsStream.size(); i++){
//                    transform to ids
                    idsStream.set(i, idsStream.get(i).split("_")[0]) ;
                }


//                List<String> idsStream = Arrays.stream(words).collect(Collectors.toList());


                String result = String.format("%s___%s___%s", "".join("=", idsStream), json.get("title").toString(), String.valueOf(words.length));
//                Text outputIds = new Text("".join("=", idsStream).
//                        join("___", json.get("title").toString()).join("___", String.valueOf(words.length))); // todo: delimiter is '='
                IntWritable docId = new IntWritable(Integer.parseInt(json.get("id").toString().trim()));
                context.write(docId, new Text(result));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }


    public static class IndexerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable docid, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String prepOne = "";
            String prepTwo = "";

            StringBuilder sb = new StringBuilder();
            String sep = "";
            int splits = 0;
            for (Text st : values) {
                String[] s = st.toString().split("___");
                splits +=1;
                prepOne = s[1];
                prepTwo = s[2];
                sb.append(sep).append(s[0]);
                sep = "=";
            }

            Map<String, Long> count = Arrays.stream(sb.toString().split("=")).collect(Collectors.
                    groupingBy(e -> e, Collectors.counting()));

            sb = new StringBuilder();
            sep = "";
            for(String s: count.keySet()){
                sb.append(sep).append(String.format("%s,%s", s, count.get(s).toString()));
                sep = "=";
            }

            String result = String.format("%s___%s___%s", sb.toString(), prepOne,
                    String.valueOf(Integer.parseInt(prepTwo)*splits));
            context.write(docid, new Text(result));
        }

    }


    public static String concatStringsWSep(Iterable<Text> strings, String separator) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (Text st : strings) {
            String s = st.toString();
            sb.append(sep).append(s);
            sep = separator;
        }
        return sb.toString();
    }


    private static void run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("_path", args[1]);
        Job job = Job.getInstance(conf, "SAMARAAAAAA word count");
        List<Path> cacheFiles = Helper.getPathsByName(conf, args[1]);
        for (Path p : cacheFiles)
            job.addCacheFile(p.toUri());

        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMapper.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void printHelp() {
        System.out.println("Indexer must get the following 5 arguments:");
        System.out.println("1st: Path to directory containing text corpus");
        System.out.println("2nd: Directory for Word Enumerator output");
        System.out.println("3rd: Directory for Document Count output");
        System.out.println("4th: Directory for AverageDocLength output");
        System.out.println("5th: Directory for Indexer output");
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input for all
//        1 argument is output for wordEnumerator output
//        2 argument is for Document Count output
//        3 argument is for AverageDocLength output
//        4 argument is for Indexer Output
        if (args[0].equals("--help")) {
            printHelp();
        } else if (args.length != 5) {
            System.out.println("Invalid number of arguments\n");
            printHelp();
        } else {
            int r1 = WordEnumerator.run(args);
            String[] args2 = {args[0], args[3]};
            int r2 = AverageDocLength.run(args2);

            run(args);
        }
    }

}
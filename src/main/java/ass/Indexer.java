package ass;

//https://github.com/kckusal/F19-IU-Big-Data-Assignment-1
//Inspired by  https://github.com/gauravsinghaec/HADOOP-DISTRIBUTED-CACHE

// Document Count = TokenizerMapper + IntSumCombiner + IntSumReducer
// Word Enumerator = TokenizerMapper + IntSumCombiner + WordEnumeratorReduce
// Vocabulary
// Indexer

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


public class Indexer {

    public static class IndexerMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void readFile(FileSystem fs, Path path, Configuration conf) throws IOException {

            byte[] bytes = IOUtils.readFullyToByteArray(fs.open(path));
            String content = new String(bytes, StandardCharsets.UTF_8);

            for (String line : content.split("\n")) {
                String[] parts = line.split("\t");
                conf.set(parts[0], parts[1]);
            }
        }


        protected void setup(Mapper.Context context) throws IOException {
            try {
                Configuration conf = context.getConfiguration();
                FileSystem fs = FileSystem.get(conf);
                // the second boolean parameter here sets the recursion to true
                RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(context.getConfiguration().get("_path")), false);
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();
            Configuration conf = context.getConfiguration();

            try {
                JSONObject json = (JSONObject) parser.parse(line);
                String[] words = json.get("text").toString().replaceAll("[^a-zA-Z ]", " ").
                        trim().split("\\s+");

                List<String> idsStream = Arrays.stream(words).map(conf::get).collect(Collectors.toList());

                Text outputIds = new Text("".join("=", idsStream)); // todo: delimiter is '='
                IntWritable docId = new IntWritable(Integer.parseInt(json.get("id").toString().trim()));
                context.write(docId, outputIds);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }


    public static class IndexerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable docid, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] merged = concatStringsWSep(values, "=").split("=");
            List<IntWritable> mergedInt = Arrays.stream(merged).map(s-> new IntWritable(Integer.parseInt(s.trim()))).collect(Collectors.toList());

            Map<IntWritable, Long> counts = mergedInt.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
//           sorting by keys
            Map<IntWritable, Long> sorted = new TreeMap<IntWritable, Long>(counts);
            List<IntWritable> keys = new ArrayList<IntWritable>(sorted.keySet());

            StringJoiner sb = new StringJoiner("=");
            for(IntWritable st: keys) {
                String s = String.format("(%s,%d)", st.toString(), sorted.get(st).intValue() );
                sb.add(s);
            }
            context.write(docid, new Text(sb.toString()));
        }

    }

    public static String concatStringsWSep(Iterable<Text> strings, String separator) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for(Text st: strings) {
            String s = st.toString();
            sb.append(sep).append(s);
            sep = separator;
        }
        return sb.toString();
    }


    public static List<Path> getPathsByName(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(path), false);
        List<Path> pathArr = new ArrayList<Path>();
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            Path loc = fileStatus.getPath();
            if (loc.getName().startsWith("part-r-")) {
                pathArr.add(loc);
            }
        }

        return pathArr;
    }

    private static void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("_path", args[1]);
        Job job = Job.getInstance(conf, "word count");
        List<Path> cacheFiles = getPathsByName(conf, args[1]);
        for (Path p : cacheFiles)
            job.addCacheFile(p.toUri());

        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMapper.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int r1 = WordEnumerator.run(args);
        run(args);
    }

}

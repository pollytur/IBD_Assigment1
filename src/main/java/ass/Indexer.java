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

    public static Integer maxIds = 0;

    public static void readFile(FileSystem fs, Path path, Configuration conf) throws IOException {

        byte[] bytes = IOUtils.readFullyToByteArray(fs.open(path));
        String content = new String(bytes, StandardCharsets.UTF_8);
        Integer maxId = 0;
        for (String line : content.split("\n")) {
            String[] parts = line.split("\t");
            Integer cur = Integer.parseInt(parts[1].split("_")[0]);
            if (cur>maxId){
                maxId = cur;
            }
            conf.set(parts[0], parts[1]);
        }
        maxIds = maxId;
    }

    protected static void setupPlaceholder ( Configuration conf) throws IOException {
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

        @Override
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

                Text outputIds = new Text("".join("=", idsStream)); // todo: delimiter is '='
                IntWritable docId = new IntWritable(Integer.parseInt(json.get("id").toString().trim()));
                context.write(docId, outputIds);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }


    public static class IndexerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            setupPlaceholder(conf);

        }


        public void reduce(IntWritable docid, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] merged = concatStringsWSep(values, "=").split("=");
            context.write(docid, new Text(inside_reduce(merged)));
        }

    }

    public static String inside_reduce(String[] merged){

        HashMap<IntWritable,Double> hm = new HashMap<IntWritable,Double>();
        for(String s : merged){
            String[] splitted = s.split("_");
            hm.put(new IntWritable(Integer.parseInt(splitted[0])), Double.parseDouble(splitted[1]));
        }

        List<IntWritable> mergedInt = Arrays.stream(merged).map(s-> new IntWritable(Integer.
                parseInt(s.split("_")[0].trim()))).collect(Collectors.toList());

        Map<IntWritable, Long> counts = mergedInt.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
//           sorting by keys
        Map<IntWritable, Long> sorted = new TreeMap<IntWritable, Long>(counts);
        List<IntWritable> keys = new ArrayList<IntWritable>(sorted.keySet());

        StringJoiner sb = new StringJoiner("=");
        for (int i =0; i<maxIds; i++){
            if (sorted.keySet().contains(new IntWritable(i))){
                Double val = ((sorted.get(new IntWritable(i)).intValue())/hm.get(new IntWritable(i)));
                sb.add(val.toString());
            }
            else{
                sb.add("0");
            }
        }
        return sb.toString();
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
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
//        0 argument is input for all
//        1 argument is output for wordEnumerator output
//        2 argument is for Document Count output
//        3 argument is for Indexer Output
        int r1 = WordEnumerator.run(args);
//        String[] argsCleared = new String[]{args[0], args[2]};
//        int r2 = DocumentCount.run(argsCleared);
        run(args);
    }

}

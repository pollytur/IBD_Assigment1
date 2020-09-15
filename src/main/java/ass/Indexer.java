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

import jdk.internal.net.http.common.Pair;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Indexer {



//    public static class FrequencySortMap extends  Mapper<Object, Text, IntWritable, Text>
//    {
//
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
//        {
//            String line = value.toString();
//            String[] arrOfStr  = line.split(" ");
//            int frequency = Integer.parseInt(arrOfStr[1]);
//            String word = arrOfStr[0];
//            context.write(new IntWritable(frequency), new Text(word));
//
//        }
//    }


    public static class IndexerMapper extends Mapper<Object, Text, IntWritable, List<IntWritable>>{

        String fileName ;
        IdParser metadata;
        private final static IntWritable one = new IntWritable(1);

        protected void setup(Mapper.Context context) throws IOException
        {
            try{
                Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                for (Path eachPath : localFiles) {
//                fileName = eachPath.getName().toString().trim();
                    fileName = eachPath.getName().trim();
                    if (fileName.equals("word_ids.txt"))
                    {
                        metadata = new IdParser();
                        metadata.initialize(eachPath);
                        break;
                    }
                }
//            System.out.println("File : "+ localFiles[0].toString());
            }
            catch(NullPointerException e)
            {
                e.printStackTrace();
            }

//        System.out.println("Cache : "+context.getConfiguration().get("mapred.cache.files"));
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{

            String line = value.toString().toLowerCase();
            JSONParser parser = new JSONParser();

            try {
                JSONObject json = (JSONObject) parser.parse(line);
                String[] words = json.get("text").toString().replaceAll("[^a-zA-Z ]", "").split("\\s+");
                List< IntWritable> encoding = null ;
                for (int i = 0; i < words.length; i++) {
                    encoding.add(new IntWritable(metadata.hm.get(i)));
                }
                context.write(new IntWritable(Integer.parseInt(json.get("id").toString().trim())), encoding);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }

    public static class IndexerReducer extends Reducer<IntWritable, List<IntWritable>,
            IntWritable, List<Pair<IntWritable, IntWritable>>>{

        public void reduce(IntWritable docid, List<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            Map<IntWritable, Long> counts = values.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
//           sorting by keys
            Map<IntWritable, Long> sorted = new TreeMap<IntWritable, Long>(counts);
            List<IntWritable> keys = new ArrayList<IntWritable>(sorted.keySet());
            List<Pair<IntWritable, IntWritable>> result = Collections.emptyList();

            for (int i=0; i<=keys.size(); i++){
                result.add(new Pair<IntWritable, IntWritable>(keys.get(i),
                        new IntWritable(sorted.get(keys.get(i)).intValue()) ));
            }
            context.write(docid, result);

        }

    }



    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(Indexer.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

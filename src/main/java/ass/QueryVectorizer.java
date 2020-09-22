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

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class QueryVectorizer {

    Configuration conf;
    Path path;

    QueryVectorizer(Configuration conf, String folder) {
        this.conf = conf;
        this.path = new Path(folder);
    }

    /**
     * Get mapping of words to their IDs and IDFs
     */
    private HashMap<String, String> getMap() throws Exception {
        HashMap<String, String> ret = new HashMap<String, String>();

        FileSystem fs = FileSystem.get(this.conf);
        RemoteIterator<LocatedFileStatus> fsIterator = fs.listFiles(this.path, false);

        // TODO

        return ret;
    }

    /**
     * get id and frequency from wordID_wordIDF
     */
    public static int[] parseMapping(String pair) {
        int[] ans = {0, 0};

        String[] nums = pair.split("_");

        ans[0] = Integer.parseInt(nums[0]);
        ans[1] = Integer.parseInt(nums[1]);

        return ans;
    }

    public String vectorize(String query) throws Exception {
        HashMap<String, String> dic = getMap();

        // for each query word, store its TF/IDF score
        HashMap<String, Integer> scores = new HashMap<String, Integer>();

        for (String word : query.split(" ")) {
        }
        
        return "";
    }
}

package ass;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class IdParser {
    private String stationMetadata;
    public HashMap<String, Integer> hm = new HashMap<String, Integer>();

    public void initialize(Path path) throws IOException {
        FileReader fr = new FileReader(path.toString());
        BufferedReader buff = new BufferedReader(fr);
        while ((stationMetadata = buff.readLine()) != null) {
            String s[] = stationMetadata.split(" ");
            if (s[0] != " " && s[1] != " ") {
                hm.put(s[0].trim(), Integer.parseInt(s[1].trim()));
            }
        }
    }
}

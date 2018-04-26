import java.io.BufferedReader;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.*;
import java.io.StringReader;
import java.util.Scanner;

public class ScanWordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // The output of the mapper is a map from words (including duplicates) to the value 1.

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        // The key is the character offset within the file of the start of the line, ignored.
        // The value is a line from the file.

        String line = value.toString();
        StringReader sr = new StringReader(line);
        BufferedReader br = new BufferedReader(sr);
        JsonReader reader = Json.createReader(br);
        JsonObject tweet = reader.readObject();
        JsonObject entities = tweet.getJsonObject("entities");

        JsonArray urls = entities.getJsonArray("urls");

        if(urls.size()>0){
            for (int i=0; i<urls.size(); i++){
                JsonObject url = urls.getJsonObject(i);
                if(url.containsKey("expanded_url")){
                    try{
                        String urlText = url.get("expanded_url").toString();
                        output.write(new Text(urlText), new LongWritable(1));
                    }
                    catch(Exception e){}
                }
            }
        }
    }
}
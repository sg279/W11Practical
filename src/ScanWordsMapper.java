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

        //Parse the value parameter (representing one line of the data) to a string
        String line = value.toString();
        //Create a new string reader called sr with the line string as a parameter
        StringReader sr = new StringReader(line);
        //Create a new buffered reader from the string reader
        BufferedReader br = new BufferedReader(sr);
        //Create a new json reader from the buffered reader
        JsonReader reader = Json.createReader(br);
        //Create a new json object called tweet using the JsonReader
        JsonObject tweet = reader.readObject();
        //Create a json object called entities from the entities property in the tweet object
        JsonObject entities = tweet.getJsonObject("entities");

        //Try the following
        try {
            //If the entities object contains a property called urls do the following
            if (entities.containsKey("urls")) {
                //Create a json array from the objects in the urls property
                JsonArray urls = entities.getJsonArray("urls");
                //For each item in the urls array do the following
                for (int i = 0; i < urls.size(); i++) {
                    //If item isn't null create a new json object from the item
                    if (urls.getJsonObject(i) != null) {
                        JsonObject url = urls.getJsonObject(i);
                        //If the url object contains a property called expanded_url do the following
                        if (url.containsKey("expanded_url")) {
                            //Parse the expanded_url property value to a string
                            String urlText = url.get("expanded_url").toString();
                            //If the expanded_url text isn't null, write the value to the output with a new LongWitable value of 1
                            if (!urlText.equals("null")) {
                                output.write(new Text(urlText), new LongWritable(1));
                            }
                        }
                    }
                }
            }

        }
        //Catch any exceptions thrown
        catch (Exception e) {
        }
    }

}
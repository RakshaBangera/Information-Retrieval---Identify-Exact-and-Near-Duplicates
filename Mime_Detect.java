/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package mime_detect;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author prashanth
 */
public class Mime_Detect{
    
   public static List<String> extractUrls(String text)
{
    List<String> containedUrls = new ArrayList<>();
    String urlRegex = "((Content-Type=).*)";
    Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
    Matcher urlMatcher = pattern.matcher(text);

    while (urlMatcher.find())
    {
        containedUrls.add(text.substring(urlMatcher.start(0),
                urlMatcher.end(0)));
    }

    return containedUrls;
}
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
       BufferedReader br = null;
       Map<String, Integer> map = new HashMap<>();
       List <String> url;
        try {
            br = new BufferedReader(new FileReader(args[0]));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Mime_Detect.class.getName()).log(Level.SEVERE, null, ex);
        }
        String line;
           try {
               while ((line = br.readLine()) != null) {
                   url = extractUrls(line);
                   for(String ext : url) {
                       String mediaType = ext.replaceAll("Content-Type=", "");
                       Integer count = map.get(mediaType);
                       map.put(mediaType, (count == null) ? 1 : count + 1);
                   }       }
           } catch (IOException ex) {
               Logger.getLogger(Mime_Detect.class.getName()).log(Level.SEVERE, null, ex);
           }
        try {
            br.close();
        } catch (IOException ex) {
            Logger.getLogger(Mime_Detect.class.getName()).log(Level.SEVERE, null, ex);
        }
        Map<String, Integer> treeMap = new TreeMap<>(map);
        printMap(treeMap);
	
        System.out.println("Done MIME Detection !!!!");
}

    private static void printMap(Map<String, Integer> map) {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Type : " + entry.getKey() + "\t\t\t Count : "
			+ entry.getValue());
        
        }
    }
}

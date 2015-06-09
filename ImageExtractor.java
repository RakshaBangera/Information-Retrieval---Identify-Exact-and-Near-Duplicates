/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imageextractor;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.imageio.ImageIO;
import static javax.imageio.ImageIO.write;
import sun.net.www.content.image.png;

/**
 *
 * @author prashanth
 */
public class ImageExtractor {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        PrintWriter writer = null;
        try {
            // TODO code application logic here
            int count = 1111;
            BufferedReader br = null;
            Map<String, Integer> map = new HashMap<>();
            List <String> url;
            writer = new PrintWriter("the-file-name.txt", "UTF-8");
            new File(System.getProperty("user.dir")+"/images/").mkdir();
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
                        String mediaType = ext.replace("[", "").replace("]", "");
                        //   Integer count = map.get(mediaType);
                        //   map.put(mediaType, (count == null) ? 1 : count + 1);
                        //   Map<String, Integer> treeMap = new TreeMap<>(map);
                        //   printMap(treeMap);
                        
                        imageDownloader(mediaType,count++);
                        System.out.println(mediaType +"-->>"+ count);
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(ImageExtractor.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(Mime_Detect.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Done MIME Detection !!!!");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ImageExtractor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(ImageExtractor.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            writer.close();
        }
         
 
    }

    private static void imageDownloader(String imageurl, int count)
    {       
        BufferedImage image =null; 
        StringBuilder sb = new StringBuilder("Image");
        sb.append("_");
        sb.append(count);
        String workingdirectory = System.getProperty("user.dir");
        
        try{
 
            URL url =new URL(imageurl);
            // read the url
           image = ImageIO.read(url);
           
 
            ImageIO.write(image, "jpg",new File(workingdirectory+"/images/"+sb.toString()+".jpg"));
 
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    
    private static List<String> extractUrls(String text) {
        
        List<String> containedUrls = new ArrayList<>();
    String urlRegex = "^https?://(?:[a-z\\-]+\\.)+[a-z]{2,6}(?:/[^/#?]+)+\\.(?:jpg)";
    Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
    Matcher urlMatcher = pattern.matcher(text);

    while (urlMatcher.find())
    {
        containedUrls.add(text.substring(urlMatcher.start(0),
                urlMatcher.end(0)));
    }

    return containedUrls;
    }

    private static void printMap(Map<String, Integer> map) {
         System.out.println("\n\n\n\n\n\n");
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Type : " + entry.getKey() + "\t\t\t Count : "
			+ entry.getValue());
        
        }
    }
    
}

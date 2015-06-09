package org.apache.nutch.urlfilter.nearduplicates;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*; 
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.net.URLFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.nutch.metadata.Metadata;

public class NearDuplicatesURLFilter implements URLFilter 
{
	  private static final Logger LOG = LoggerFactory.getLogger(NearDuplicatesURLFilter.class);
	  private Configuration conf;
	  private Configuration config;
	  /* neardup TreeMap to hold Hash(key) and URL(value)*/
      private static TreeMap<String, String> neardup = new TreeMap<String, String>();
      /* KnowledgeBase(KB) is a HashMap to hold previously crawled DataSet  */
      private static Map<String, String> KB = new HashMap<String, String>(1024);
	  private static int nearDupCount = 0; 
	  private static int exactDupCount = 0; 
	  public NearDuplicatesURLFilter() 
	  {
	      	LOG.info("Inside NearDuplicatesURLFilter");
	  }


	  public String filter(String urlString)
        {
			String metadata = null;
			String url = null;
			String url1,url2;
			String regex = "((js)|(css))$";
			Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
			Matcher m = pattern.matcher(urlString);
			/* Filter out CSS and JS*/
			if(m.find())
			{
				LOG.info("NearDuplicatesURLFilter: Filtered css and js");
				return null;
			}
			
			/* User path to KnowledgeBase */
			
			metadata = readSegment("/Users/vinaykumar/desktop/nutch/nutch/runtime/local/merged/20150226151933", urlString);
			
			/* If metadata found check if near duplicate*/
			if (metadata != null) 
			{ 
				/* Calculate SimHash value for the metadata */
				long simhash1 = SimHash.simhash64(metadata);
				LOG.info("NearDuplicatesURLFilter: URL :"+urlString);
				LOG.info("NearDuplicatesURLFilter: Simhash :"+simhash1);
			    
				/* Check if the obtained SimHash is already present in the TreeMap */
				if (neardup.containsKey(Long.toString(simhash1)))
				{	
					/* Iterate through TreeMap to find the matching URL */
					for (Entry<String, String> entry : neardup.entrySet()) 
					{
						if (entry.getKey().equals(Long.toString(simhash1))) 
						{
							url = entry.getValue();
						}
					}
					
					/* Eliminate revisited URLs */
					if(url != null)
					{
						url1 = urlString.substring(0, urlString.length() - (urlString.endsWith("/") ? 1 : 0));
						url2 = url.substring(0, url.length() - (url.endsWith("/") ? 1 : 0));
						if(url != null && !url1.equals(url2))
						{
							exactDupCount++;
							LOG.info("NearDuplicatesURLFilter:"+urlString+" is filtered as duplicate of "+url);
							LOG.info("NearDuplicatesURLFilter: Exact Duplicate Count = "+exactDupCount);
						}
					}				
					return null;
				}
				else
				{
					/* Get urls which have similar metadata into an List*/
					ArrayList<String> nearArr = IsNearDuplicate(Long.toString(simhash1));	
					
					/* If no duplicate found */
					if(!(nearArr.size() >0))
					{
						/* Add the Simhash-URL value into TreeMap*/
						neardup.put(Long.toString(simhash1),urlString);
						return urlString;
					}
					else
					{
						nearDupCount++;
						/* Find the similar URLS*/
						for(int i=0;i<nearArr.size();i++)
						{
							LOG.info("NearDuplicatesURLFilter:"+urlString+" is filtered as near duplicate of "+neardup.get(nearArr.get(i)));
							LOG.info("NearDuplicatesURLFilter: Near Duplicate Count = "+nearDupCount);
						}
						return null;
					}	
				}
			}
			else
			{
				/* If Metadata not found go ahead with crawling */
				LOG.info("NearDuplicatesURLFilter: No Metadata! Allow URL to be Crawled!");
				return urlString;
			}
		
        }

	
	/**
	 * 
	 * @param key
	 * @return List of URLs if near duplicate found else empty List.
	 * Checks for 3 floor and ceiling values of key for nearest neighbor.
	 * If size is less than 5 
	 */
	  public ArrayList<String> IsNearDuplicate(String key)
	  {
		  /* Set of floor values to the Current Simhash */
		  Set<String> floorKeys = new LinkedHashSet<String>();
		  /* Set of Ceil values to the Current Simhash */
		  Set<String> ceilingKeys = new LinkedHashSet<String>();
		  /* List of near duplicate urls */
		  ArrayList<String> nearduplicates = new ArrayList<String>();
		
		  if(neardup.size() > 0)
		  {
			  int neighbor_count = 0;
			  if(neardup.size() > 4 )
				  neighbor_count = 4;
			  else
				  neighbor_count = neardup.size();
			  /* if floor key present */
			  if(neardup.floorKey(key) != null)
			  {
				  floorKeys.add(neardup.floorKey(key));
				  String floorKey = neardup.floorKey(key);
				  /* Getting the 5 adjacent keys on both sides of key i.e lower than and higher than key to calculate hamming distance*/
				  for(int i=0;i<neighbor_count;i++)
				  {
					  if(neardup.floorKey(floorKey) != null)
					  {
						  floorKeys.add(neardup.floorKey(floorKey));
						  floorKey = neardup.floorKey(floorKey);
					  }		
				  }
				/**
				 * Check if there is a near neighbor. If found return true.
				 */
				  if(floorKeys.size() > 0)
				  {
					  for(String floorkey:floorKeys)
					  {
						  /* Calculate hamming distance for similarity */
						  int distance = SimHash.hammingDistance(Long.parseLong(key),Long.parseLong(floorkey));
						  if(distance < 3)
							  nearduplicates.add(floorkey);
					  }
				  }
			  }
			
			  /* if ceil key present */
			  if(neardup.ceilingKey(key) != null)
			  {
				  ceilingKeys.add(neardup.ceilingKey(key));
				  String ceilingKey = neardup.ceilingKey(key);

				  /* Getting the 5 adjacent keys on both sides of key i.e lower than and higher than key to calculate hamming distance*/
				  for(int i=0;i<neighbor_count;i++)
				  {
					  if(neardup.ceilingKey(ceilingKey) != null)
					  {
						  ceilingKeys.add(neardup.ceilingKey(ceilingKey));
						  ceilingKey = neardup.ceilingKey(key);
					  }
				  }
				/**
				 * Check if there is a near neighbor. If found return true.
				 */
				  if(ceilingKeys.size() > 0)
				  {
					  for(String ceilingkey:ceilingKeys)
					  {
						  /* Calculate hamming distance for similarity */
						  int distance = SimHash.hammingDistance(Long.parseLong(key),Long.parseLong(ceilingkey));
						  if(distance < 3)
						  {
							  nearduplicates.add(ceilingkey);
						  }
					  }
				  }
			  }
			  return nearduplicates;
		  }
		  return nearduplicates;
	  }
		
	  public static void main(String[] args) throws Exception 
	  {
		  NearDuplicatesURLFilter dup =  new NearDuplicatesURLFilter();
	  }
	 
	  public Configuration getConf() 
	  {
		  return conf;
	  }
 
	  public void setConf(Configuration conf) 
	  {
		  this.conf = conf;
		  String pluginName = "urlfilter-nearduplicates";
		  Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(URLFilter.class.getName()).getExtensions();
		  for (int i = 0; i < extensions.length; i++) 
		  {
			  Extension extension = extensions[i];
		  }
	  }
	
	  public String readSegment(String segPath,String url)
	  {  
		  String retMetadata = null;
		  if(KB.isEmpty())
		  {      
			  try 
			  {
				  FileSystem fs = FileSystem.get(conf);
				  /* Path to the knowledgeBase */
				  Path file = new Path(segPath, Content.DIR_NAME + "/part-00000/data");
				  SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
				  Text key = new Text();
				  Content content = new Content();
				  while (reader.next(key, content)) 
				  {
					  if (content != null && content.getMetadata()!= null)
					  {
						  /* Get metadata from the segment files */
						  Metadata m=new Metadata();
						  m=content.getMetadata();
						  retMetadata =m.toString();
						  KB.put(key.toString(), retMetadata);
					  }
				  } 
				  reader.close();
				  fs.close();
			  }
			  catch (IOException e) 
			  { 
				  LOG.info("ExactDuplicatesURLFilter: IOException occurred");
			  } 
		  }
		  return KB.get(url);
	  }
}


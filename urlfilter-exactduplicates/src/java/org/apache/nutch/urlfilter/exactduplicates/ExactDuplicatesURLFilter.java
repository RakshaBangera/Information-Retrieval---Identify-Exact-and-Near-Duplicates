package org.apache.nutch.urlfilter.exactduplicates;

//JDK imports
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

//Nutch and Hadoop imports
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

public class ExactDuplicatesURLFilter implements URLFilter {

	  private static final Logger LOG = LoggerFactory
	      .getLogger(ExactDuplicatesURLFilter.class);
	  private Configuration conf;
	  /* Hash Map for identifying exact duplicates */
      private static Map<String, String> exactdup  = new HashMap<String, String>(1024);
      /* Hash Map which stores the previous crawl data set */
      private static Map<String, String> KB = new HashMap<String, String>(1024);  
	  private static int exactDupCount = 0; 
	  
	  public ExactDuplicatesURLFilter() 
	  {
		  LOG.info("ExactDuplicatesURLFilter: ExactDuplicatesURLFilter Invoked");
	  }


	public String filter(String urlString)
    {
		String content = null;
		String regex = "((js)|(css))$";
		String url1,url2;
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(urlString);
		if(m.find())
		{
			/* Ignore CSS and JS */
			return null;
		}
		/* Fetch content from the previously crawled data set if available */
		String home = System.getProperty("user.home");
		content = readSegment(home+"/dataset/timestamp",urlString);
		String sha1 = null;
		if ( content != null)
		{
			/* Trim spaces from the content before forming the MD5 digest */
			sha1 = formDigest(content.replaceAll("\\s", ""));
		}
		else
		{
			/* If content is not available from previous data set, use urlString itself to form the digest */
			LOG.info("ExactDuplicatesURLFilter: Using url to form digest:");
			sha1 = formDigest(urlString);
		}
		LOG.info("URL: "+urlString);
		LOG.info("SHA-1 Hash: "+sha1);
		
		/* If the computed SHA-1 digest is already present in the hash map then we have found a duplicate */
		if (exactdup.containsValue(sha1))
		{	
			String url = null;
			for (Entry<String, String> entry : exactdup.entrySet()) {
				if (entry.getValue().equals(sha1)) {
					url = entry.getKey();
				}
			}
			/* Normalize URLs ending with slash(/) */
			url1 = urlString.substring(0, urlString.length() - (urlString.endsWith("/") ? 1 : 0));
			url2 = url.substring(0, url.length() - (url.endsWith("/") ? 1 : 0));
			if(url != null && !url1.equals(url2))
			{
				exactDupCount++;
				LOG.info("ExactDuplicatesURLFilter: "+urlString+" is filtered as duplicate of "+url);
				LOG.info("ExactDuplicatesURLFilter: Duplicate Count = "+exactDupCount);
			}
			/* Duplicate found */
			return null;
		}
		else
		{
			/* urlString is not a duplicate */
			/* Store urlString and its SHA-1 digest in a hash map */
			exactdup.put(urlString, sha1);
			return urlString;
		}
	}
		
	public static void main(String[] args) throws Exception 
	{
		ExactDuplicatesURLFilter dup =  new ExactDuplicatesURLFilter();
	} 
	public Configuration getConf() 
	{
        	return conf;
    }
	public void setConf(Configuration conf) 
	{
		this.conf = conf; 
		String pluginName = "urlfilter-exactduplicates";
		Extension[] extensions = PluginRepository.get(conf)
		.getExtensionPoint(URLFilter.class.getName()).getExtensions();
		for (int i = 0; i < extensions.length; i++) 
		{
			Extension extension = extensions[i];
		}
 	}
	/* Compute SHA-1 Digest of the content/urlString */
	private static String formDigest(String msg)
	{	
		MessageDigest digest = null;
		try 
		{
			digest = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) 
		{
			LOG.info("Exception: NoSuchAlgorithmException");
		}
		byte[] hashedBytes = null;
		try 
		{
			digest.update(msg.getBytes("UTF-8"),0, msg.getBytes("UTF-8").length);
			hashedBytes = digest.digest();
		} catch (UnsupportedEncodingException e) {
			LOG.info("Exception: UnsupportedEncodingException");
		}
 		return toHexString(hashedBytes);
	}

	/* Convert SHA-1 digest to Hex String for comparison */
	private static String toHexString(byte[] arrayBytes) 
	{
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) 
		{
		    stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
    }
	
	/* Read the page content of the "url" passed from the previous data set stored in "segPath" */
	public String readSegment(String segPath,String url)
	{  
		String retContent = null;
		/* First time read the previous data set from segPath and store all records in a Hash Map */
		/* KB is the knowledge base. Its a hash map<url, content */
		/* KB enables faster look-up of content from second time onwards */
		if(KB.isEmpty())
		{	      
			try 
			{
				FileSystem fs = FileSystem.get(conf);
				Path file = new Path(segPath, Content.DIR_NAME + "/part-00000/data");
			    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
				Text key = new Text();
				Content content = new Content();
		        // Loop through sequence files
				while (reader.next(key, content)) {
					if (content != null && content.getContent()!= null)
					{
						retContent = new String(content.getContent(), "UTF-8");
						/* Populate KB <url, content> */
						KB.put(key.toString(), retContent);
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



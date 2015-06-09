package org.apache.nutch.urlfilter.nearduplicates;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class SimHash {

	public static int hammingDistance(long hash1, long hash2) {
		long i = hash1 ^ hash2;
		i = i - ((i >>> 1) & 0x5555555555555555L);
		i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
		i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
		i = i + (i >>> 8);
		i = i + (i >>> 16);
		i = i + (i >>> 32);
		return (int) i & 0x7f;
	}

	public static long simhash64(String doc) {
		int bitLen = 64;
		int[] bits = new int[bitLen];
		List<String> tokens = tokens(doc);
		for (String t : tokens) {
			long v = hash64(t);
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					++bits[i - 1];
				else
					--bits[i - 1];
			}
		}
		long hash = 0x0000000000000000;
		long one = 0x0000000000000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 1) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}
	
	
	public static long hash64(String doc) {
		byte[] buffer = doc.getBytes(Charset.forName("utf-8"));
		ByteBuffer data = ByteBuffer.wrap(buffer);
		return hash64(data, 0, buffer.length, 0);
	}

	public static long hash64(ByteBuffer key, int offset, int length, long seed) {
		long m64 = 0xc6a4a7935bd1e995L;
		int r64 = 47;

		long h64 = (seed & 0xffffffffL) ^ (m64 * length);

		int lenLongs = length >> 3;

		for (int i = 0; i < lenLongs; ++i) {
			int i_8 = i << 3;

			long k64 = ((long) key.get(offset + i_8 + 0) & 0xff)
					+ (((long) key.get(offset + i_8 + 1) & 0xff) << 8)
					+ (((long) key.get(offset + i_8 + 2) & 0xff) << 16)
					+ (((long) key.get(offset + i_8 + 3) & 0xff) << 24)
					+ (((long) key.get(offset + i_8 + 4) & 0xff) << 32)
					+ (((long) key.get(offset + i_8 + 5) & 0xff) << 40)
					+ (((long) key.get(offset + i_8 + 6) & 0xff) << 48)
					+ (((long) key.get(offset + i_8 + 7) & 0xff) << 56);

			k64 *= m64;
			k64 ^= k64 >>> r64;
			k64 *= m64;

			h64 ^= k64;
			h64 *= m64;
		}

		int rem = length & 0x7;

		switch (rem) {
		case 0:
			break;
		case 7:
			h64 ^= (long) key.get(offset + length - rem + 6) << 48;
		case 6:
			h64 ^= (long) key.get(offset + length - rem + 5) << 40;
		case 5:
			h64 ^= (long) key.get(offset + length - rem + 4) << 32;
		case 4:
			h64 ^= (long) key.get(offset + length - rem + 3) << 24;
		case 3:
			h64 ^= (long) key.get(offset + length - rem + 2) << 16;
		case 2:
			h64 ^= (long) key.get(offset + length - rem + 1) << 8;
		case 1:
			h64 ^= (long) key.get(offset + length - rem);
			h64 *= m64;
		}

		h64 ^= h64 >>> r64;
		h64 *= m64;
		h64 ^= h64 >>> r64;

		return h64;
	}
	
	public static List<String> tokens(String doc) {
		List<String> binaryWords = new LinkedList<String>();
		for(int i = 0; i < doc.length() - 1; i += 1) {
			StringBuilder bui = new StringBuilder();
			bui.append(doc.charAt(i)).append(doc.charAt(i + 1));
			binaryWords.add(bui.toString());
		}
		return binaryWords;
	}
	
	
	public static void main(String[] args)  {

        String str1 = "fgfgfgfgfg";
        String str2 = "fgfgfgfgfg";

        
        long hash1 = simhash64(str1);
        long hash2 = simhash64(str2);
        System.out.println(hash1);
        System.out.println(hash2);

        int diff = 0;

        diff = hammingDistance(hash1,hash2);
        System.out.println("Document 1 and 2 differ by " + diff);
        

}
}

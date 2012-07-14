package util;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;


public class MD5Verifier {

	/**
	 * Verify is a md5 file 
	 * @param args
	 */
	public static boolean verifyMD5(FileInputStream source, FileInputStream hash) throws IOException{
		boolean result = false;

		String md5 = DigestUtils.md5Hex(source);

		byte[] bytes=new byte[hash.available()];
		hash.read(bytes);

		result = md5.equals(new String(bytes));

		return result;
	}

	/**
	 * Verify is a md5 file 
	 * @param args
	 */
	public static boolean verifyMD5(String sourceStr, String hashStr) throws IOException{
		boolean result = false;

		FileInputStream source = new FileInputStream(sourceStr);
		String md5Source = DigestUtils.md5Hex(source);

		FileInputStream hash = new FileInputStream(hashStr);
		byte[] bytes=new byte[hash.available()];
		hash.read(bytes);
		String md5Hash = new String(bytes);

		result = md5Source.equals(md5Hash);

		source.close();
		hash.close();

		return result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if(args.length != 2)
			throw new RuntimeException("Invalid args");

		try {
			System.out.println(verifyMD5(args[0], args[1]));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
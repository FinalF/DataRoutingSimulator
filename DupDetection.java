
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

public class DupDetection {

	Map<String, long[]> index;
	long totalChunkNum;
	 int chunkSize;
	 int segSize;
	 long chunkNum;
	 String[] hashRecord;
	 File sampledHash; //the input file
	 BloomFilter<String> bf;
	 long dup;
	 long total;
	static int[] chunksizeRecord ;
	long indexSize;
	int segAmount;
	int boundaryCheck;
	 DupDetection(Map<String, long[]> index,int chunkSize, int segSize, String[] hashRecord,
			BloomFilter<String> bf,File sampledHash, int  segAmount,double[] op){
		this.sampledHash = sampledHash;
		this.index = index;
		this.chunkSize =chunkSize;
		this.segSize = segSize;
		this.hashRecord = hashRecord;
		this.bf = bf;
		this.chunkNum = segSize*1024/chunkSize;
		 chunksizeRecord = new int[(int) chunkNum];	 		 
		 this.segAmount = segAmount;

	}
	
	 void dedup() throws IOException{
		 /*
		  * Load the input file into the index, 
		  * compare to find out duplicate
		  */
	 
		Scanner loadIn;
		loadIn = new Scanner(sampledHash);
		loadIn.nextLine();
		int i = 0;
		while(loadIn.hasNextLine()){
			totalChunkNum++;
		String[] infor = loadIn.nextLine().split(",");
		hashRecord[i] = infor[4]; //put the hashvalue into the RAM for the following comparison
		chunksizeRecord[i] = Integer.parseInt(infor[3]) ;
		total += Long.parseLong(infor[3]);
		long hashvalue = Long.parseLong(infor[4].substring(30),16);
			if(index.containsKey(infor[4])){

				/*
				 * Here we can set a threshold to limit the cache size!!
				 */
				
				index.put(infor[4],meta); //update the index
			}
		i++;
		}

	 }
	 

		long getTotalChunkNum(){
			return totalChunkNum;
		}
		
		long getTotal(){
			return total;
		}
		
		long getDup(){
			return dup;
		}
		
		
		
		//count the total number of lines(except the 1st line)
		static long getTotalLines(String file) throws IOException {
	        Scanner in = new Scanner(new File(file));
	        long lines = 0;
	        while(in.hasNextLine()){
	        	lines++;
	        	in.nextLine();
	        }
	        in.close();
	        return lines;
	    }
		
//	//method to check the lower n bits of fingerprint to determine boundary
//	private boolean check(long fingerprint) {
//		boundaryCheck = (int) (Math.log(sampleRate) / Math.log(2));
//	    int i = 0;
//	    boolean check = true;
//	    do {
//	        check = (fingerprint & (1L << i)) == 0;
//	        if (!check) {
//	            return false;
//	        }
//	        i++;
//	    } while (i < boundaryCheck);
//	    return true;
//	}
	

}


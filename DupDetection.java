
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class DupDetection {

	Map<String,Integer> index;
	long totalChunkNum;
	 int chunkSize;
	 int segSize;
	 long chunkNum;
	 File sampledHash; //the input file
	 long dup;
	 long total;
	long indexSize;
	int segAmount;
        double[] op;
	 DupDetection(Map<String, Integer> index,int chunkSize, int segSize, 
			File sampledHash, int  segAmount,double[] op){
		this.sampledHash = sampledHash;
		this.index = index;
		this.chunkSize =chunkSize;
		this.segSize = segSize;
		this.chunkNum = segSize*1024/chunkSize; 		 
		 this.segAmount = segAmount;
                 this.op = op;

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
                    total += Long.parseLong(infor[3]);
                            if(index.containsKey(infor[4])){
                                dup+=Long.parseLong(infor[3]);		
                            }else{
                                index.put(infor[4],1); //update the index
                            }
		}
                op[0] += getTotal();
                op[1] += getDup();
                op[2] = op[1]/op[0];
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


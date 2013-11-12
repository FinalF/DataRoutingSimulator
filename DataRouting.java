import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/*
 * simulator of data routing in deduplication cluster
 * Use the intermediate data generated from DedupSimulator 
 */
public class DataRouting {
/*
 * parameters: 
 * 1. the number of storage nodes : n
 * 2. the number of initial tries : m
 * 3. the number of operations: t
 */
	public static int t = 1;
	public static int n = 2;
	public static int m = 10;
	public static int M = 1; //number of containers to be tracked in the index (num of segments to be compared)
	public static int chunksize = 8;
	public static int segsize = 16;
	public static String resultRecordFolder = "testData/resultRecord";
	public static Queue<Node> nodeQue=new LinkedList<Node>();
	public static File[] recordFiles = new File(resultRecordFolder).listFiles();
	public static PrintWriter resultRecord; 
	public static String[] hashRecord;

	
	public static int segAmount = 100;  //segment, every waveAmount segments of data, calculate dedup rate  & record index size
	static long starttime;
	static long endtime;	
	
	public static File fileManage = new File("testData/FileManager/fileInforRecord.txt");
	public static File ChunkManage = new File("testData/chunkManage/chunkInfo");
	public static String outputpath = "testData/outputSegSample";  //folder where store the input data table
	public static long totalChunkNum = 0;
	public static long fileTotal;
	public static long total;
	public static long dup;	
		
	
	public static ArrayList<BloomFilter<String>> BF;
	public static ArrayList<HashMap<String, long[]>> INDEX;
	public static ArrayList<double[]> OP;
	
	public static void main(String[] args) throws IOException, InterruptedException {
	
/*----------------------------------Read in parameters----------------------------------------*/		
			int i = 0;
				if(i<args.length){
					n = Integer.parseInt(args[i]);
					System.out.println("Total "+ n +" storage nodes");
				}else{
					System.out.println("Number of nodes required");
				}
			i++;
				if(i < args.length){
				m = Integer.parseInt(args[i]);
				System.out.println("Explore "+m+" times");
				}else{System.out.println("Number of initial tests required");
				}

			/*Initialize index and BF for each node*/
			BF = new ArrayList<BloomFilter<String>>(n);
			INDEX = new ArrayList<HashMap<String, long[]>>(n);
			OP = new ArrayList<double[]>(n);
			for(int j=0; j<n ; j++){
				 BF.add(j, new BloomFilter<String>(0.1, 7864320/n));
				 INDEX.add(j, new HashMap<String, long[]>());
				 double[] num = {0.0,0.0,0.0};  //total data-dup data-dedup ratio
				 OP.add(j,num);
			}
			
			/*Process files(all the segments*/
			File[] incomingFile = null;
			incomingFile = new File(outputpath).listFiles();
			for(int h = 1; h <=incomingFile.length; h++){
				File file = null;
				file = new File(outputpath+"/Segment_"+ h);
				hashRecord = new String[segsize*1024/chunksize];

				
			/*
			 * Part I: deduplication
			 */

			resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
			resultRecord.flush();
			resultRecord.println("\nThe chunk size is: " + chunksize + " KB"
					+ "\nThe segment size is: " + segsize + " MB");		
			System.out.println("\nThe chunk size is: " + chunksize
					+ "\nThe segment size is: " + segsize);	
		
			/*
			 * Data routing operation
			 * Before m, send it to all
			 * After that, send it to one with highest dedup ratio
			 */
			if(t<=m){
				for(int k=0;k<n;k++){
					dedupProcess(file,BF.get(k),INDEX.get(k),OP.get(k));
				}
			}else{
				int choice = StochasticDataRoute();
				dedupProcess(file,BF.get(choice),INDEX.get(choice),OP.get(choice));
			}
			
			/*
			 * periodically output statistics (every 'segAmount' segments, we do one time's dedup)
			 */
			if(t%segAmount == 0){	
				nodeQue.add(new Node(total,dup,indexTotal(),(double)dup/total,(double)dup/indexTotal(),OP));
				resultRecord.println();
				resultRecord.println("The culmulative segments are: " + i +
									"\nCurrent data amount is: "+total+
									"\nThe duplicates are: "+dup+
									"\nThe current index size is: " + indexTotal()+
									"\nThe deduplication rate is : " + (double)dup/total*100 +"%");
				resultRecord.println();
				resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexTotal())+" dup/index entry slot");	
			}					
			}
			resultRecord.println("The total data is: "+total+"\nThe duplicates are: "+dup+
					 "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
			
			System.out.println("The total data is: "+total+"\nThe duplicates are: "+dup+
								"\nThe deduplication rate is : " + (double)dup/total*100 +"%");


			nodeQue.add(new Node(total,dup,indexTotal(),(double)dup/total,(double)dup/(indexTotal()),OP));
	
			matlabStatistic();
			
			resultRecord.close();

			
			}

	static int StochasticDataRoute(){
			int choice = -1;
			double max = -1;
			int count = 0;
			Iterator<double[]> ir = OP.iterator();
			while(ir.hasNext()){				
				double[] num = ir.next();
				if(num[2]>max){
					choice = count ;
					max=num[2];
				}
				count++;
			}
			return choice;
	}

	static long indexTotal(){
		long size = 0;
		for(int i = 0; i < INDEX.size(); i++){
			size+=INDEX.get(i).size();
		}
		return size;
	}
	
	
	static void dedupProcess(File file, BloomFilter<String> bf,HashMap<String, long[]> index,double[] op) throws IOException{
	
		DupDetection dP = new DupDetection(index,chunksize, segsize, hashRecord,
				bf,file,segAmount,op);		
		dP.dedup();
		total += dP.getTotal();
		dup += dP.getDup();

		totalChunkNum += dP.getTotalChunkNum();

		resultRecord.println("The current index size is: " + indexTotal());
		
		System.out.println("The current index size is: " + indexTotal());
//		System.out.println("The current BloomFilter size is: " + bf.size());		
		


		
		
	}

	static void matlabStatistic(){
		resultRecord.println("\n\n++++++++++++++++For matlab++++++++++\n");
		resultRecord.println("Total,dup,index size,dedupRatio,dedupEfficiency\n");
		for(Node n:nodeQue){
			resultRecord.print(n.total);
			resultRecord.print(",");
			resultRecord.print(n.dup);
			resultRecord.print(",");

			resultRecord.print(n.size);
			resultRecord.print(",");
			resultRecord.print(n.dedupR);
			resultRecord.print(",");
			resultRecord.print(n.dedupE);
			resultRecord.println();
		}

		resultRecord.println("\n+++++++++++++++++++++statistics for each node++++++++++\n");
		for(int i = 0; i < n; i++){
			Iterator<Node> ir = nodeQue.iterator();
			while(ir.hasNext()){
				Node n = ir.next();
				double dedupRatio = n.OP.get(i)[2];
				resultRecord.print(dedupRatio);
			}
			resultRecord.println();

		}
	}

}

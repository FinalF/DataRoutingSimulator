import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;

/*
 * simulator of data routing in deduplication cluster
 * Use the intermediate data generated from DedupSimulator 
 */
public class DataRouting {
/*
 * parameters: 
 * 1. the number of storage nodes : n
 * 2. the number of initial tries : m
 */
	public static int n = 2;
	public static int m = 10;
	public static int mode = 0;
	public static int chunksize = 8;
	public static int segsize = 16;
	public static String resultRecordFolder = "testData/resultRecord";
	public static Queue<Node> nodeQue=new LinkedList<Node>();
	public static File[] recordFiles = new File(resultRecordFolder).listFiles();
	public static PrintWriter resultRecord; 

	
	public static int segAmount = 200;  //segment, every waveAmount segments of data, calculate dedup rate  & record index size
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
	public static ArrayList<HashMap<String, Integer>> INDEX;
	public static ArrayList<double[]> OP;
	
	public static void main(String[] args) throws IOException, InterruptedException {
	
/*----------------------------------Read in parameters----------------------------------------*/		
                        resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
			resultRecord.flush();	
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
                        i++;
				if(i < args.length){
				mode = Integer.parseInt(args[i]);
                                    if(mode==0){
                                             resultRecord.println("Stochastic approach--- "+n+" node --- "+m+" explorations");                                    
                                    }else if(mode==1){
                                            resultRecord.println("Stateless  "+n+" node --- "+m+" explorations");
                                    }else if(mode==2){
                                            resultRecord.println("Single node, heuristic solution");
                                    }
				}else{System.out.println("Choose mode: 0. MAB based\n1. stateless\n2. statefull");
				}
			/*Initialize index and BF for each node*/
			BF = new ArrayList<BloomFilter<String>>(n);
			INDEX = new ArrayList<HashMap<String, Integer>>(n);
			OP = new ArrayList<double[]>(n);
			for(int j=0; j<n ; j++){
//				 BF.add(j, new BloomFilter<String>(0.1, 4000000/n));
				 INDEX.add(j, new HashMap<String, Integer>());
				 double[] num = {0.0,0.0,0.0};  //total, data-dup, data-dedup ratio
				 OP.add(j,num);
			}
                        

			resultRecord.println("\nThe chunk size is: " + chunksize + " KB"
					+ "\nThe segment size is: " + segsize + " MB");		
			System.out.println("\nThe chunk size is: " + chunksize
					+ "\nThe segment size is: " + segsize);	
                        
			/*Process files(all the segments*/
			File[] incomingFile = new File(outputpath).listFiles();
			for(int h = 1; h <=incomingFile.length; h++){
                            File file = new File(outputpath+"/Segment_"+ h);
                            System.out.println("Procssing "+h+" th segment");
			/*
			 * Data routing operation
			 * Before m, send it to all
			 * After that, send it to one with highest dedup ratio
			 */
                        if(mode==0){
                            /*my algorithm*/

                            if(h<=m){
                                    for(int k=0;k<n;k++){
                                            dedupProcess(file,INDEX.get(k),OP.get(k));
                                    }
                            }else{
                                    int choice = StochasticDataRoute();
                                    dedupProcess(file,INDEX.get(choice),OP.get(choice));
                            }
                        }else if(mode==1){
                            /*stateless,hash(1st chunk) mod n*/

                            int choice = StatelessDataRoute(file);
                            dedupProcess(file,INDEX.get(choice),OP.get(choice));
                        }else if(mode==2){
                            /*heuristic*/
                            dedupProcess(file,INDEX.get(0),OP.get(0));
                        }else{
                            System.out.println("Mode chosen error!");
                            resultRecord.close();
                            System.exit(0);
                        }
			/*
			 * periodically output statistics (every 'segAmount' segments, we do one time's dedup)
			 */
			if(h%segAmount == 0){	
				nodeQue.add(new Node(total,dup,indexTotal(),(double)dup/total,(double)dup/indexTotal(),tmpData()));
                                resultRecord.println();
				resultRecord.println("The culmulative segments are: " + h +
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

                        ArrayList<double[]> tmp = OP;
			nodeQue.add(new Node(total,dup,indexTotal(),(double)dup/total,(double)dup/(indexTotal()),tmpData()));
                        tmp.clear();
			matlabStatistic();
			
			resultRecord.close();

			
			}

        static int StatelessDataRoute(File file) throws FileNotFoundException{
            int choice = 0;
                Scanner loadIn = new Scanner(file);
                loadIn.nextLine();
                String[] infor = loadIn.nextLine().split(",");
//                System.out.println("The hash is "+infor[4]);
                int tmp = Integer.parseInt(infor[4].substring(34),16);
                choice = tmp % n;
                return choice;
        }
	static int StochasticDataRoute(){
			int choice = -1;
			double max = -1;
			int count = 0;
            double avgTotal;// = total/n;
			Iterator<double[]> ir = OP.iterator();
			while(ir.hasNext()){				
				double[] num = ir.next();
				if(num[2]/(num[0]-num[1])>max){
					choice = count ;
					max=num[2]/(num[0]-num[1]);
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
	
	
	static void dedupProcess(File file,HashMap<String,Integer> index,double[] op) throws IOException{
	
		DupDetection dP = new DupDetection(index,chunksize, segsize, file,segAmount,op);		
		dP.dedup();
		total += dP.getTotal();
		dup += dP.getDup();

		totalChunkNum += dP.getTotalChunkNum();
	
		
	}

        static double[][] tmpData(){
            double[][] tmp = new double[n][3];
                for(int i = 0; i < n; i++){
                    for(int j = 0; j < 3; j++){
                        tmp[i][j] = OP.get(i)[j];          
                    }
                }
           return tmp;
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
			resultRecord.print(new DecimalFormat(".##").format(n.dedupR*100));
			resultRecord.print(",");
			resultRecord.print(new DecimalFormat(".##").format(n.dedupE*100));
			resultRecord.println();
		}

		resultRecord.println("\n+++++++++++++++++++++statistics for each node++++++++++\n");
		resultRecord.println("Dedup ratio:\n");
                for(int i = 0; i < n; i++){
			Iterator<Node> ir = nodeQue.iterator();
			while(ir.hasNext()){
				Node n = ir.next();
				double dedupRatio = n.op[i][2];
				resultRecord.print(dedupRatio);
                                resultRecord.print(",");                                
			}
			resultRecord.println();
		}
 		resultRecord.println("Amount of data received:\n");               
                for(int i = 0; i < n; i++){
			Iterator<Node> ir = nodeQue.iterator();
			while(ir.hasNext()){
				Node n = ir.next();
				double dataAmount = n.op[i][0];
				resultRecord.print(dataAmount);
                                resultRecord.print(",");                                
			}
			resultRecord.println();
		}
                
                resultRecord.println("\nAmount of duplicate data:\n");               
                for(int i = 0; i < n; i++){
			Iterator<Node> ir = nodeQue.iterator();
			while(ir.hasNext()){
				Node n = ir.next();
				double dataAmount = n.op[i][1];
				resultRecord.print(dataAmount);
                                resultRecord.print(",");                                
			}
			resultRecord.println();
		}
	}

}

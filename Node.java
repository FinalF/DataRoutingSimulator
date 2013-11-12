import java.util.ArrayList;

/*
 * used to store statistic information
 */
public class Node {
	/*
	 * 1. The amount of data has been processes
	 * 2. The amount of duplicate data

	 * 4. The index size
	 * 5. The dedup ratio
	 * 6. The dedup efficiency
	 * 7. The double[] recording the dup and total data for each node

	 */
	
	 long total;
	 long dup;	
	 long size;
	 double dedupR;
	 double dedupE;
	 ArrayList<double[]> OP;
	 
	 Node(){
		 
	 }
	 
	 Node(long total, long dup, long size, double dedupR, double dedupE,ArrayList<double[]> OP){
		 this.total = total;
		 this.dup = dup;
		 this.size = size;
		 this.dedupR = dedupR;
		 this.dedupE = dedupE;
		 this.OP = OP;
	 }

}

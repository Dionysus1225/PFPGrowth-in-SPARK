package PFPGrowth_in_SPARK;

import java.util.HashMap;
import java.util.List;

import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * Parallel FPGrowth in SPARK
 *
 */
public class Main 
{
	public static JavaSparkContext sc;
	public static final int NUM_GROUPS_DEFAULT = 100;
	public static long FLIST_SIZE = 0;

	public static void main( String[] args )
	{

		createSparkContext();
		JavaRDD<String> transactions=sc.textFile("records1305291335898081541.tmp",minPartitions());
		JavaRDD<NumericLine> parsedTransactions = transactions.map(new ParseRecordLine());
		//1.parallel counting
		//1.1 map 
		JavaRDD<Integer> items = parsedTransactions.flatMap(
				new FlatMapFunction<NumericLine, Integer>() {
					public Iterable<Integer> call(NumericLine line) { 
						return line.getValues(); 
					}});
		JavaPairRDD<Integer, Integer> pairs = items.mapToPair(new ItemsMapper());
		//1.2 reduce - generate GLists
		JavaPairRDD<Integer, Integer> counts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) { 
						return a + b; 
					}});

		FLIST_SIZE=counts.count();
		//testing:
		List<Tuple2<Integer,Integer>> tempList=counts.toArray();
		for (Tuple2<Integer,Integer> tuple : tempList){
			System.out.println("Item " + tuple._1 + " has support " + tuple._2);
		}
		System.out.println("Total number of items: "+tempList.size());

		//2.parallel fpgrowth
		
		//2.1 return for each item its gourpID and Transaction Tree - like in ParallelFPGrowthMapper
		//parsedTransactions.mapToPair(new ParallelMapper());
		//3.aggregating algorithm

	}
	private static void createSparkContext() {

		//System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//System.setProperty("spark.kryo.registrator", "fimEntityResolution.MyRegistrator");
		System.setProperty("spark.executor.memory", "5g");
		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		int numOfCores = runtime.availableProcessors();		
		SparkConf conf = new SparkConf();
		conf.setMaster("local["+numOfCores+"]");
		conf.setAppName("MFIBlocks");
		sc=new JavaSparkContext(conf);
		System.out.println("SPARK HOME="+sc.getSparkHome());

	}

	private static int minPartitions() {
		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		int numOfCores = runtime.availableProcessors();
		return numOfCores*3; //JS: Spark tuning: minSplits=numOfCores*3
	}

	/***
	 * Parser extends Function<String, CandidateBlock> for parsing lines from the text files in SPARK.
	 */
	static class ParseRecordLine implements Function<String, NumericLine> {

		public NumericLine call(String line) {
			return new NumericLine(line);
		}
	}

	static class ItemsMapper implements PairFunction<Integer, Integer, Integer> {

		public Tuple2<Integer, Integer> call(Integer item) { 
			return new Tuple2<Integer, Integer>(item, 1); 
		}
	}
	
	static class ParallelMapper implements PairFunction<NumericLine, Integer, List<Integer>> {
		private final OpenObjectIntHashMap<Integer> fMap = new OpenObjectIntHashMap<Integer>();
		//private final IntWritable wGroupID = new IntWritable();
		
		public Tuple2<Integer, List<Integer>> call(NumericLine line) { 
			//Tuple2<Integer, TransactionTree> retVal=new Tuple2<Integer, TransactionTree>(null,null);
			List<Integer> items = line.getValues();
			 OpenIntHashSet itemSet = new OpenIntHashSet();

			    for (Integer item : items) {
			      if (fMap.containsKey(item) && item!=null) {
			        itemSet.add(fMap.get(item));
			      }
			    }

			    IntArrayList itemArr = new IntArrayList(itemSet.size());
			    itemSet.keys(itemArr);
			    itemArr.sort();
			    
			    OpenIntHashSet groups = new OpenIntHashSet();
			    for (int j = itemArr.size() - 1; j >= 0; j--) {
			      // generate group dependent shards
			      int item = itemArr.get(j);
			      int groupID = getGroupID(item);
			        
			      if (!groups.contains(groupID)) {
			        IntArrayList tempItems = new IntArrayList(j + 1);
			        tempItems.addAllOfFromTo(itemArr, 0, j);
			        //context.setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: " + item);
			        //retVal._1=groupID;
			        //retVal._2= new TransactionTree(tempItems, 1L);
			      }
			      groups.add(groupID);
			    }
			return null;//new Tuple2<Integer, Integer>(item, 1); 
		}
		//TODO: we have to return tuple <groupID, TransactionTree> for each item
		//so we can map lines to pairs <item, line> and each pair map to required tuple..
		
		private int getGroupID (int item){
			int maxPerGroup = (int)FLIST_SIZE/NUM_GROUPS_DEFAULT;
			return item/maxPerGroup;
		}
	}


}

package PFPGrowth_in_SPARK;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
		JavaRDD<String> transactions=sc.textFile("records8713821771304460493.tmp",minPartitions());
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
		//converting to fmap
		OpenObjectIntHashMap<Integer> fMap = new OpenObjectIntHashMap<Integer>();
		List<Tuple2<Integer,Integer>> tempList=counts.toArray();
		for (Tuple2<Integer,Integer> tuple : tempList){
			fMap.put(tuple._1, tuple._2);
		}

		//testing:================================================
		//		List<Tuple2<Integer,Integer>> tempList=counts.toArray();
		//		for (Tuple2<Integer,Integer> tuple : tempList){
		//			
		//			
		//			System.out.println("Item " + tuple._1 + " has support " + tuple._2);
		//		}
		//		System.out.println("Total number of items: "+tempList.size());
		//========================================================

		//2.parallel fpgrowth

		//2.1 return for each item its gourpID and Transaction Tree - like in ParallelFPGrowthMapper
		JavaPairRDD<Integer, TransactionTree> gIDtransTree=parsedTransactions.flatMapToPair(new ParallelMapper(fMap,FLIST_SIZE,NUM_GROUPS_DEFAULT));

		//testing==========================================================
		//		List<Tuple2<Integer, TransactionTree>> newList=gIDtransTree.toArray();
		//		for (Tuple2<Integer,TransactionTree> tuple : newList){
		//			System.out.println("GroupID " + tuple._1 + " has tree " + tuple._2.toString());
		//		}
		//Q: groupid is unique or not? yes for each Ti
		//==================================================================

		//2.2 apply fpgrowth for each tree like in ParallelFPGrowthReducer
		//must get pattern (set) and its support 
		JavaPairRDD<List<Integer>,Integer> patternSupport=gIDtransTree.reduce(new ParallelReducer(fMap) );

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

	static class ParseRecordLine implements Function<String, NumericLine> {

		private static final long serialVersionUID = 1L;

		public NumericLine call(String line) {
			return new NumericLine(line);
		}
	}

	static class ItemsMapper implements PairFunction<Integer, Integer, Integer> {

		private static final long serialVersionUID = 1L;

		public Tuple2<Integer, Integer> call(Integer item) { 
			return new Tuple2<Integer, Integer>(item, 1); 
		}
	}

	static class ParallelMapper implements PairFlatMapFunction<NumericLine, Integer,TransactionTree> {

		private static final long serialVersionUID = 1L;
		private OpenObjectIntHashMap<Integer> fMap = new OpenObjectIntHashMap<Integer>();
		//private OpenIntHashSet groups = new OpenIntHashSet();
		private long flistSize;
		private int numPerGroup;

		//private final IntWritable wGroupID = new IntWritable();

		public ParallelMapper(OpenObjectIntHashMap<Integer> fMap, long flistSzie, int numPerGroup){
			this.fMap=fMap;
			this.flistSize=flistSzie;
			this.numPerGroup=numPerGroup;
		}

		public Iterable<Tuple2<Integer, TransactionTree>> call(NumericLine line)
				throws Exception {
			List<Tuple2<Integer,TransactionTree>> retVal=new ArrayList<Tuple2<Integer,TransactionTree>>();
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
					retVal.add(new Tuple2<Integer, TransactionTree>(groupID, new TransactionTree(tempItems, 1L)));
				}
				groups.add(groupID);
			}
			return retVal;
		}

		private int getGroupID (int item){
			int maxPerGroup = (int)flistSize/numPerGroup;
			return item/maxPerGroup;
		}
	}
	
	static class ParallelReducer implements PairFunction<NumericLine, Integer,TransactionTree> {
		//private static final long serialVersionUID = 1L;
		private OpenObjectIntHashMap<Integer> fMap = new OpenObjectIntHashMap<Integer>();
		//private OpenIntHashSet groups = new OpenIntHashSet();
		//private long flistSize;
		//private int numPerGroup;

		//private final IntWritable wGroupID = new IntWritable();

		public ParallelReducer(OpenObjectIntHashMap<Integer> fMap){
			this.fMap=fMap;
		}

		public Tuple2<Integer, TransactionTree> call(NumericLine line)
				throws Exception {
			
			
			
			TransactionTree cTree = new TransactionTree();
		    for (TransactionTree tr : values) {
		      for (Pair<IntArrayList,Long> p : tr) {
		        cTree.addPattern(p.getFirst(), p.getSecond());
		      }
		    }
		    
		    List<Pair<Integer,Long>> localFList = Lists.newArrayList();
		    for (Entry<Integer,MutableLong> fItem : cTree.generateFList().entrySet()) {
		      localFList.add(new Pair<Integer,Long>(fItem.getKey(), fItem.getValue().toLong()));
		    }
		    
		    Collections.sort(localFList, new CountDescendingPairComparator<Integer,Long>());
		    
		    if (useFP2) {
		      FPGrowthIds.generateTopKFrequentPatterns(
		          cTree.iterator(),
		          freqList,
		          minSupport,
		          maxHeapSize,
		          PFPGrowth.getGroupMembers(key.get(), maxPerGroup, numFeatures),
		          new IntegerStringOutputConverter(
		              new ContextWriteOutputCollector<IntWritable, TransactionTree, Text, TopKStringPatterns>(context),
		              featureReverseMap)
		      );
		    } else {
		      FPGrowth<Integer> fpGrowth = new FPGrowth<Integer>();
		      fpGrowth.generateTopKFrequentPatterns(
		          new IteratorAdapter(cTree.iterator()),
		          localFList,
		          minSupport,
		          maxHeapSize,
		          Sets.newHashSet(PFPGrowth.getGroupMembers(key.get(),
		                                                         maxPerGroup, 
		                                                         numFeatures).toList()),
		          new IntegerStringOutputConverter(
		              new ContextWriteOutputCollector<IntWritable,TransactionTree,Text,TopKStringPatterns>(context),
		              featureReverseMap),
		          new ContextStatusUpdater<IntWritable,TransactionTree,Text,TopKStringPatterns>(context));
		    }
		  }
			return null;
		}
	
	}	

}

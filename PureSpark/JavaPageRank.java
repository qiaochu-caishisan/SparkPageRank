package com.sparkPageRank.pageRank;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class JavaPageRank implements Serializable {

	private static final long serialVersionUID = 3662916104012783823L;
	public static final double DELTA = 0.85;
	public static final double NON_CONVERGING_FACTOR = 0.01;
	public static final double DIFF = 0.001;
	public static final double ZERO = 0.0;
	public static final double ONE = 1.0;

	public int run(String dir, String output) {
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRankSpark" + System.currentTimeMillis());
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> raw = sc.textFile(dir + "/*");

		
		JavaPairRDD<Long, Node> adjList = raw
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, Node>() {
					private static final long serialVersionUID = -3764092888326577981L;
					public Iterator<Tuple2<Long, Node>> call(Iterator<String> value) throws Exception {
						ArrayList<Tuple2<Long, Node>> graph = new ArrayList();
						while (value.hasNext()) {
							String v = value.next();
							if (v == null || "".equals(v))
								continue;
							String[] list = v.split(",");
							Long srcId = Long.parseLong(list[0]);
							Node node = null;
							if (list.length == 2) {
								node = new Node(srcId, null);
							} else {
								long[] destIds = new long[list.length - 2];
								for (int i = 2; i < list.length; i++) {
									destIds[i - 2] = Long.parseLong(list[i]);
								}
								node = new Node(srcId, destIds);
							}
							graph.add(new Tuple2<Long, Node>(srcId, node));
						}
						return graph.iterator();
					}
				});
		adjList.persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		long PAGES = adjList.count();
		System.out.println("Pages:" + PAGES);
		long nonConvergingPages = PAGES;
		
		JavaPairRDD<Long, Double> pageRank = adjList.mapValues(new Function<Node, Double>() {
			private static final long serialVersionUID = 6419553615194608424L;
			public Double call(Node arg0) throws Exception {
				return ONE;
			}
		});
		
		int counter = 0;
		final DoubleAccumulator PARTIAL_PR_ACCU = sc.sc().doubleAccumulator("PARTIAL_PR");
		final LongAccumulator NON_CONVERGING_PAGES_ACCU = sc.sc().longAccumulator("NON_CONVERGING_PAGES");
		while (nonConvergingPages >= PAGES * NON_CONVERGING_FACTOR) {
			System.out.println("iter:" + (counter++));
			System.out.println("nonConvergingPages:" + nonConvergingPages);
			PARTIAL_PR_ACCU.reset();
			NON_CONVERGING_PAGES_ACCU.reset();
			
			JavaPairRDD<Long, Double> partialPR = adjList
					.join(pageRank)
					.mapPartitionsToPair(
					new PairFlatMapFunction<Iterator<Tuple2<Long, Tuple2<Node, Double>>>, Long, Double>() {
						private static final long serialVersionUID = 6387806599947414237L;
						public Iterator<Tuple2<Long, Double>> call(Iterator<Tuple2<Long, Tuple2<Node, Double>>> nodes)
								throws Exception {
							List<Tuple2<Long, Double>> result = new ArrayList();
							while (nodes.hasNext()) {
								Tuple2<Long, Tuple2<Node, Double>> tuple = nodes.next();
								long srcId = tuple._1;
								Node node = tuple._2._1;
								double srcPr = tuple._2._2;
							
								result.add(new Tuple2<Long, Double>(srcId, ZERO));
								if (!node.isDanglingNode()) {
									
									double p = srcPr / node.getDestIds().length;
									for (Long destId : node.getDestIds()) {
										result.add(new Tuple2<Long, Double>(destId, p));
									}
								}
							}
							return result.iterator();
						}
					})
					.reduceByKey(new Function2<Double, Double, Double>() {
						private static final long serialVersionUID = -8910876946046811474L;
						public Double call(Double arg0, Double arg1) throws Exception {
							return arg0 + arg1;
						}
					});
			
			partialPR.foreach(new VoidFunction<Tuple2<Long,Double>>() {
				private static final long serialVersionUID = -3169774123452324432L;
				public void call(Tuple2<Long, Double> arg0) throws Exception {
					PARTIAL_PR_ACCU.add(arg0._2);
				}
			});
			final double danglingPr = (PAGES - PARTIAL_PR_ACCU.value())/PAGES;
			System.out.println("PR：" + danglingPr*PAGES);
			
			JavaPairRDD<Long, Double> fullPR = partialPR
					.mapValues(new Function<Double, Double>() {
						private static final long serialVersionUID = 4480176021165466325L;
						public Double call(Double partialPR) throws Exception {
							
							return (1 - DELTA + DELTA * (danglingPr + partialPR));
						}
					});
			
			fullPR.join(pageRank).foreach(new VoidFunction<Tuple2<Long,Tuple2<Double,Double>>>() {
				private static final long serialVersionUID = -707275790932530868L;
				public void call(Tuple2<Long, Tuple2<Double, Double>> arg0) throws Exception {
					if(Math.abs(arg0._2._1-arg0._2._2)>DIFF){
						
						NON_CONVERGING_PAGES_ACCU.add(1);
					}
				}
			});
			nonConvergingPages = NON_CONVERGING_PAGES_ACCU.value();
			System.out.println("nonConvergingPages：" + nonConvergingPages);
			pageRank = fullPR;
		}
		pageRank.saveAsTextFile(output);
		sc.close();
		return 0;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		JavaPageRank task = new JavaPageRank();
		String input = args[0];
		String output = "/out/" + System.currentTimeMillis();
		long start = System.currentTimeMillis();
		task.run(input, output);
		System.out.println("PageRank total time:" + (System.currentTimeMillis() - start) + "ms");
	}
	class Node implements Serializable {
		private static final long serialVersionUID = -8385191780501513828L;
		private long srcId;
		private boolean isDanglingNode = false;
		private long[] destIds;
		public Node(long srcId, long[] destIds){
			this.srcId = srcId;
			this.destIds = destIds;
			if(destIds ==null||destIds.length==0){
				this.isDanglingNode = true;
			}
		}
		public long getSrcId() {
			return srcId;
		}
		public long[] getDestIds() {
			return destIds;
		}
		public boolean isDanglingNode() {
			return isDanglingNode;
		}
	}
}

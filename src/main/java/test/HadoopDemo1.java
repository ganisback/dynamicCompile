package test;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetupCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class HadoopDemo1 {
	//job example, count the number of vertices
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(HadoopDemo1.class);
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		BaseConfiguration configuration = new BaseConfiguration();
		final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil
				.makeHadoopConfiguration(configuration);
		ConfigHelper.setInputColumnFamily(hadoopConfiguration, "bigluck3", "edgestore");
		ConfigHelper.setInputInitialAddress(hadoopConfiguration, "localhost");
		ConfigHelper.setInputPartitioner(hadoopConfiguration, "Murmur3Partitioner");
		SlicePredicate predicate = new SlicePredicate();
		final int rangeBatchSize = Integer.MAX_VALUE;
		predicate.setSlice_range(getPropertySlice(rangeBatchSize));

		ConfigHelper.setInputSlicePredicate(hadoopConfiguration, predicate);

		Class<?> aClass = CqlInputFormat.class;

		JavaRDD<String> javaPairRDD = jsc
				.newAPIHadoopRDD(hadoopConfiguration, (Class<InputFormat<Long, Row>>) aClass, Long.class, Row.class)
				.map(x -> {
					return x._2.getClass().toString();
				});
		long t1 = System.currentTimeMillis();
		long count = javaPairRDD.count();
		logger.info("Total nodes={}", count);
		long t2 = System.currentTimeMillis();
		logger.warn("Total Cost = [{}] ms", (t2 - t1));
		spark.stop();
	}

	public static SliceRange getPropertySlice(final int limit) {
		StaticBuffer[] bounds = IDHandler.getBounds(RelationCategory.PROPERTY, false);

		final SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(bounds[0].asByteBuffer());
		sliceRange.setFinish(bounds[1].asByteBuffer());
		sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getLimit()));
		return sliceRange;
	}

	public static SliceRange getEdgeSlice(final int limit) {
		StaticBuffer[] bounds = IDHandler.getBounds(RelationCategory.EDGE, false);

		final SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(bounds[0].asByteBuffer());
		sliceRange.setFinish(bounds[1].asByteBuffer());
		sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getLimit()));
		return sliceRange;
	}

	private static SliceRange getPropertySlice2(final int limit) {
		StaticBuffer[] bounds = IDHandler.getBounds(RelationCategory.PROPERTY, false);

		final SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(bounds[0].asByteBuffer());
		sliceRange.setFinish(bounds[1].asByteBuffer());
		sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getLimit()));
		return sliceRange;
	}
}
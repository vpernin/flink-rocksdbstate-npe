package org.apache.flink;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;

public class Reproducer implements Serializable {

	public static final short PARALLELISM = 8;

	@Test
	public void testState() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(PARALLELISM);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.enableCheckpointing(TimeUnit.MILLISECONDS.toMillis(50), CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend(new RocksDBStateBackend("file:///tmp"));

		DeduplicateFilter deduplicateFilter = new DeduplicateFilter();
		env
			.fromElements(
					new Tuple1<>("123456789"),
					new Tuple1<>("64657454545454"),
					new Tuple1<>("123456789"))
			.keyBy(new KeySelector<Tuple1<String>, Short>() {
				@Override
				public Short getKey(Tuple1<String> value) throws Exception {
					return (short) (MathUtils.murmurHash(value.f0.hashCode()) % PARALLELISM);
				}
			})
			.filter(deduplicateFilter)
			.print();

		env.execute();

		Assert.assertEquals(2L, deduplicateFilter.count.get());
	}

	private static class DeduplicateFilter extends RichFilterFunction<Tuple1<String>> {

		public static AtomicInteger count = new AtomicInteger();

		// De-duplicate / idempotence state, growing and growing
		private MapState<String, Long> deduplicateState;

		@Override
		public void open(Configuration parameters) throws Exception {
			deduplicateState = getRuntimeContext().getMapState(new MapStateDescriptor("deduplicate_state", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

			// This call throws a NPE in ShortSerializer.serialize because record is null, because backend.getCurrentKey() is null in AbstractRocksDBState
			deduplicateState.entries();
		}

		@Override
		public boolean filter(Tuple1<String> value) throws Exception {
			if (deduplicateState.get(value.f0) == null) {
				deduplicateState.put(value.f0, System.currentTimeMillis());
				count.incrementAndGet();
				return true;
			}
			return false;
		}
	}
}
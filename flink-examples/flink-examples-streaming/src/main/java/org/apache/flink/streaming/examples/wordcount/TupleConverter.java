package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TupleConverter implements MapFunction<SampleEvent, Tuple3<Long, Long, Double>> {
    public Tuple3<Long, Long, Double> map(SampleEvent sample) {
        return Tuple3.of(sample.getTs(), sample.getKey(), sample.getValue());
    }
}

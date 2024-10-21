package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregate implements AggregateFunction<SampleEvent, AverageAggregate.Accumulator, Double> {

    public static class Accumulator {
        public double sum;
        public long count;
    }

    @Override
    public Accumulator createAccumulator() {
        Accumulator acc = new Accumulator();
        acc.sum = 0.0;
        acc.count = 0;
        return acc;
    }

    @Override
    public Accumulator add(SampleEvent event, Accumulator acc) {
        acc.sum += event.getValue();
        acc.count++;
        return acc;
    }

    @Override
    public Double getResult(Accumulator acc) {
        if (acc.count == 0) {
            return 0.0; // Avoid division by zero
        }
        return acc.sum; /// acc.count;
    }

    @Override
    public Accumulator merge(Accumulator acc1, Accumulator acc2) {
        acc1.sum += acc2.sum;
        acc1.count += acc2.count;
        return acc1;
    }
}

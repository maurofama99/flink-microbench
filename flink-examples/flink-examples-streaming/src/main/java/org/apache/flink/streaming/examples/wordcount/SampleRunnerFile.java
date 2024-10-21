package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.wordcount.util.CLI;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import javax.annotation.Nullable;
import java.time.Duration;


public class SampleRunnerFile {

    public static void main(String[] args) throws Exception {

        /**************** WINDOW TYPE ******************/
        final String WINDOW = "session";
        final Duration size = Duration.ofMillis(10000);
        final Duration slide = Duration.ofMillis(1000);
        final Duration session = Duration.ofMillis(2000);
        /***********************************************/

        final CLI params = CLI.fromArgs(args);

        final OutputTag<String> latencySideStream = new OutputTag<String>("latency"){};
        DataStream<SampleEvent> rawEventStream;
        SingleOutputStreamOperator<List<SampleEvent>> finalStream = null;
        // SingleOutputStreamOperator<Double> finalStream = null;

        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        env.configure(config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        rawEventStream = env
                .addSource(new SampleSource(params.getInputs().get(), Long.MAX_VALUE))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<SampleEvent>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(SampleEvent lastElement, long extractedTimestamp) {
                        return new Watermark(maxTimestampSeen);
                    }

                    private long maxTimestampSeen = 0;

                    @Override
                    public long extractTimestamp(SampleEvent temperatureEvent, long l) {
                        if (WINDOW == "sliding") {
                            long ts = temperatureEvent.getTs(); // in milliseconds
                            maxTimestampSeen = Long.max(maxTimestampSeen, ts);
                            return ts;
                        } else if (WINDOW == "session"){
                            Random rand = new Random();
                            int randomness = rand.nextInt(5000);
                            long ts = temperatureEvent.getTs() + randomness;
                            maxTimestampSeen = Long.max(maxTimestampSeen, ts);
                            return ts;
                        } else {
                            System.err.println("error in assigning timestamps");
                            return 0;
                        }
                    }
                });

        env.setBufferTimeout(-1);
        env.getConfig().enableObjectReuse();

        if (Objects.equals(WINDOW, "sliding")) {

            finalStream = rawEventStream
                    .keyBy(SampleEvent::getKey)
                    .window(SlidingEventTimeWindows.of(
                            size,
                            slide))
                    .allowedLateness(Duration.ofSeconds(10000000))
                    //.aggregate(new AverageAggregate());
                    .process(new MyProcessWindowFunction());

        } else if (Objects.equals(WINDOW, "session")){

            finalStream = rawEventStream
                    .keyBy(SampleEvent::getKey)
                    .window(EventTimeSessionWindows.withGap(
                            session))
                    .allowedLateness(Duration.ofSeconds(10000000))
                    //.aggregate(new AverageAggregate());
                    .process(new MyProcessWindowFunction());

        } else System.err.println("window not specified correctly");

        if (params.getOutput().isPresent()) {

            finalStream
                    .map(eventsList -> {
                        StringBuilder sb = new StringBuilder();
                        for (SampleEvent event : eventsList) {
                            sb.append("key: ").append(event.getKey()).append("\n");
                        }
                        return sb.toString(); // Return the formatted string representation of the event list
                    })
                    .sinkTo(
                            FileSink.<String>forRowFormat(
                                            new Path("/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/flink/dump"), // Path where the file will be written
                                            new SimpleStringEncoder<>())  // Use SimpleStringEncoder for string output
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1)) // Split file after 1MB
                                                    .withRolloverInterval(Duration.ofSeconds(10)) // Create new part every 10s
                                                    .build())
                                    .build())
                    .name("file-sink");

//                finalStream.sinkTo(
//                            FileSink.<Double>forRowFormat(
//                                            params.getOutput().get(), new SimpleStringEncoder<>())
//                                    .withRollingPolicy(
//                                            DefaultRollingPolicy.builder()
//                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
//                                                    .withRolloverInterval(Duration.ofSeconds(10))
//                                                    .build())
//                                    .build())
//                    .name("file-sink");


        } else {
            System.err.println("no output file detected, printing on stdout");
            finalStream.print().name("print-sink");
        }

        long window_size = size.getSeconds()/slide.getSeconds();
        String window_size_str = window_size+"";
        String session_gap_str = session+"";

        DataStream<String> latencyStream = finalStream.getSideOutput(latencySideStream);
        if (WINDOW == "sliding") {
            latencyStream.sinkTo(
                            FileSink.<String>forRowFormat(
                                            new Path(params.getOutput().get() + "_" + window_size_str + "_sliding"), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("file-sink-measures");
        } else {
            latencyStream.sinkTo(
                            FileSink.<String>forRowFormat(
                                            new Path(params.getOutput().get() + "_" + session_gap_str + "_session_evict_new"), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("file-sink-measures");
        }

        long start = System.currentTimeMillis();
        System.out.println(env.getExecutionPlan());
        env.execute("WindowedSamples");
        System.out.println("Time taken: " + (System.currentTimeMillis() - start) + " ms");
    }


    public static class MyProcessWindowFunction
            extends ProcessWindowFunction<SampleEvent, List<SampleEvent>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<SampleEvent> elements, Collector<List<SampleEvent>> out) {
            List<SampleEvent> eventsList = new ArrayList<>();
            for (SampleEvent event : elements) {
                eventsList.add(event);
            }
            // Emit the full list of elements for this window
            out.collect(eventsList);
        }
    }
}


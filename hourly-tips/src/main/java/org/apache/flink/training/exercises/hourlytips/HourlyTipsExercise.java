/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source);

        // replace this with your solution
        DataStream<TaxiFare> watermarkedFares = fares.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TaxiFare>forMonotonousTimestamps()
                        .withTimestampAssigner((taxiFare, oldTimeStamp) -> taxiFare.startTime.toEpochMilli())
        );

        DataStream<Tuple3<Long, Long, Float>> driversWithSummedTips = watermarkedFares.keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .reduce(new TipSum(), new ProcessTipSumWindowFunction());

        DataStream<Tuple3<Long, Long, Float>> driversWithMaximumTipsPerHour = driversWithSummedTips
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .maxBy(2);

        // the results should be sent to the sink that was passed in
        // (otherwise the tests won't work)
        // you can end the pipeline with something like this:

        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
        // hourlyMax.addSink(sink);

        driversWithMaximumTipsPerHour.addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Hourly Tips");
    }

    private static class TipSum implements ReduceFunction<TaxiFare> {
        @Override
        public TaxiFare reduce(TaxiFare value1, TaxiFare value2) throws Exception {
            float tipSum = value1.tip + value2.tip;
            return new TaxiFare(value1.rideId,
                    value1.taxiId,
                    value1.driverId,
                    value1.startTime,
                    value1.paymentType,
                    tipSum,
                    value1.tolls,
                    value1.totalFare);
        }
    }

    private static class ProcessTipSumWindowFunction extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(Long driverId, ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            // This should be the result of the previous reduce operation,
            // i.e. the TaxiFare containing the total sum of tips for each driverId, per hour
            Iterator<TaxiFare> taxiFareIterator = elements.iterator();
            Float tipSum;
            if (taxiFareIterator.hasNext()) {
                tipSum = taxiFareIterator.next().tip;
            } else {
                // Not sure how to correctly handle a missing reduce result
                // What happens when a 1-hour window contains no TaxiFare events?
                tipSum = null;
            }
            long windowEndTimestamp = context.window().getEnd();
            Tuple3<Long, Long, Float> driverWithSummedTips = new Tuple3<>(windowEndTimestamp, driverId, tipSum);
            out.collect(driverWithSummedTips);
        }
    }
}

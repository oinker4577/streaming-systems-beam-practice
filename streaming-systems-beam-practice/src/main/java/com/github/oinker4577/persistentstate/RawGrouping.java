package com.github.oinker4577.persistentstate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class RawGrouping {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawGrouping.class);
  private static final Duration ONE_MINUTE = Duration.standardMinutes(1L);
  private static final Duration TWO_MINUTE = Duration.standardMinutes(2L);

  static class ExtractTeamScore extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(@Element String element,
                               OutputReceiver<KV<String, Integer>> receiver) {
      // TODO: Clean up
      String[] record = element.split(",", -1);
      receiver.outputWithTimestamp(KV.of(record[1], Integer.valueOf(record[2])),
          Instant.parse(record[3]));
    }
  }

  public static class ParseScoreRecord
      extends PTransform<PCollection<String>, PCollection<KV<String, Integer>>> {
    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<String> lines) {

      return lines.apply(ParDo.of(new ExtractTeamScore()));
    }
  }

  public static void main(String[] args) {
    URL inputPath = RawGrouping.class.getClassLoader().getResource("userscore.csv");
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    // TODO: refactor each of this mess.
    pipeline.apply(TextIO.read().from(inputPath.getPath()))
        .apply(new ParseScoreRecord())
        // TODO: figure out watermark and processing time.
        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(TWO_MINUTE))
            .triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(ONE_MINUTE))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(TWO_MINUTE)
            .accumulatingFiredPanes())
        .apply(Sum.integersPerKey())
        .apply(ParDo.of(new DoFn<KV<String, Integer>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c)  {
            LOGGER.info("key: {}, value: {}, timestamp: {}, index: {}, timing: {}",
                c.element().getKey(),
                c.element().getValue(),
                c.timestamp(),
                c.pane().getIndex(),
                c.pane().getTiming());
          }
        }));
    pipeline.run();
  }
}

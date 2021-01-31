package com.github.oinker4577.persistentstate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class RawGrouping {
  private static final Logger logger = LoggerFactory.getLogger(RawGrouping.class);

  static class ExtractTeamScore extends DoFn<String, KV<String, Long>> {
    @ProcessElement
    public void processElement(@Element String element,
                               OutputReceiver<KV<String, Long>> receiver) {
      String[] record = element.split(",", -1);
      receiver.output(KV.of(record[1], Long.valueOf(record[2])));
    }
  }

  public static class ParseScoreRecord
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      return lines.apply(ParDo.of(new ExtractTeamScore()));
    }
  }

  public static void main(String[] args) {
    URL inputPath = RawGrouping.class.getClassLoader().getResource("userscore.csv");
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(TextIO.read().from(inputPath.getPath()))
        .apply(new ParseScoreRecord())
        .apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c)  {
            logger.info("key: {}, value: {}",
                c.element().getKey(),
                c.element().getValue());
          }
        }));
    pipeline.run();
  }
}

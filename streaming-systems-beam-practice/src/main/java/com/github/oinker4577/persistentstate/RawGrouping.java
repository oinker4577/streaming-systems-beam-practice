package com.github.oinker4577.persistentstate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class RawGrouping {
  private static final Logger logger = LoggerFactory.getLogger(RawGrouping.class);

  public static void main(String[] args) {
    URL inputPath = RawGrouping.class.getClassLoader().getResource("userscore.csv");
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(TextIO.read().from(inputPath.getPath()))
        .apply(ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c)  {
            logger.info(c.element());
          }
        }));
    pipeline.run();
  }
}

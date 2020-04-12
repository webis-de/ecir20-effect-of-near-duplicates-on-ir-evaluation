package io.anserini.collection;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.archive.archivespark.sparkling.warc.WarcLoader;
import org.archive.archivespark.sparkling.warc.WarcRecord;
import org.apache.commons.lang.StringUtils;
import org.archive.archivespark.sparkling.http.HttpMessage;

import com.google.common.collect.Iterators;

import de.webis.trec_ndd.util.S3Files;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import scala.Option;

@NoArgsConstructor
public class CommonCrawlCollection extends DocumentCollection implements SegmentProvider<ClueWeb12Collection.Document> {

  @Setter
  private S3Files s3Files;

  @Override
  public List<Path> getFileSegmentPaths() {
    return s3Files.filesInBucket().stream()
                  .map(i -> Paths.get("/" + i.getKey()))
                  .collect(Collectors.toList());
  }

  @Override
  public Segment<ClueWeb12Collection.Document> createFileSegment(Path p) throws IOException {
    Iterator<WarcRecord> responseRecords = allResponseWarcRecords(s3Files.rawContent(p.toString().substring(1)));

    return new Segment<ClueWeb12Collection.Document>() {
      @Override
      public boolean hasNext() {
        return responseRecords.hasNext();
      }

      @Override
      public ClueWeb12Collection.Document next() {
    	 return toDoc(responseRecords.next());
      }
    };
  }

  @SneakyThrows
  private static ClueWeb12Collection.Document toDoc(WarcRecord record) {
    Option<HttpMessage> httpMessage = record.http();
    String body = !httpMessage.isDefined() ? null : httpMessage.get().bodyString();
    String url = !record.url().isDefined() ? null : record.url().get();

    return toDoc(body, url);
  }
  
  private static ClueWeb12Collection.Document toDoc(String body, String url) {
    return new ClueWeb12Collection.Document() {
      @Override
      public String id() {
        return getURL();
      }

      @Override
      public boolean indexable() {
        return body != null && !StringUtils.isBlank(body) && url != null;
      }

      @Override
      public String getURL() {
        return url;
      }

      @Override
      public String content() {
        return body;
      }
    };
  }
  
  public static Iterator<WarcRecord> allResponseWarcRecords(InputStream input) {
    Iterator<WarcRecord> ret = allWarcRecords(input);

    return Iterators.filter(ret, i -> i.isResponse());
  }

  static Iterator<WarcRecord> allWarcRecords(InputStream input) {
    input = new BufferedInputStream(input);
    scala.collection.Iterator<WarcRecord> ret = WarcLoader.load(input);

    return new Iterator<WarcRecord>() {
      @Override
      public boolean hasNext() {
        return ret.hasNext();
      }

      @Override
      public WarcRecord next() {
        return ret.next();
      }
    };
  }
}

package com.cloudera.flume.demo;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.RollingFileSink;
import org.junit.Test;

public class TestCSVAvroSerializer {

  @Test
  public void testCSV() {
    MemoryChannel ch = new MemoryChannel();
    ch.configure(new Context());

    RollingFileSink s = new RollingFileSink();
    s.setChannel(ch);
    Context ctx = new Context();
    ctx.put("sink.directory", "target/test");
    ctx.put("sink.serializer", CSVAvroSerializer.Builder.class.getName());
    s.configure(ctx);

    String line = "1371782343001,1371782343023,view,65605,201.112.234.35,tgoodwin,/product/24923,/product/60444";
    Event e = EventBuilder.withBody(line, Charsets.UTF_8);

    Transaction txn = ch.getTransaction();
    txn.begin();
    ch.put(e);
    txn.commit();
    txn.close();

    try {
      s.process();
    } catch (EventDeliveryException ex) {
      ex.printStackTrace();
    }
  }

}

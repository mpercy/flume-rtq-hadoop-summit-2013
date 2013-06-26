/**
 * Copyright 2013 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.demo;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVAvroSerializer extends AbstractAvroEventSerializer<UserAction> {

  private static final Logger logger =
      LoggerFactory.getLogger(CSVAvroSerializer.class);

  private OutputStream out;

  private CSVAvroSerializer(OutputStream out) {
    this.out = out;
  }

  @Override
  protected OutputStream getOutputStream() {
    Preconditions.checkNotNull(out);
    return out;
  }

  @Override
  protected Schema getSchema() {
    Schema schema = UserAction.SCHEMA$;
    Preconditions.checkNotNull(schema);
    return schema;
  }

  @Override
  protected UserAction convert(Event event) {
    String line = new String(event.getBody(), Charsets.UTF_8).trim();
    String[] fields = line.split(",");
    UserAction action = new UserAction();
    if (isValid(fields, 0)) action.setTxnId(Long.parseLong(fields[0]));
    if (isValid(fields, 1)) action.setTs(Long.parseLong(fields[1]));
    if (isValid(fields, 2)) action.setAction(fields[2]);
    if (isValid(fields, 3)) action.setProductId(Long.parseLong(fields[3]));
    if (isValid(fields, 4)) action.setUserIp(fields[4]);
    if (isValid(fields, 5)) action.setUserName(fields[5]);
    if (isValid(fields, 6)) action.setPath(fields[6]);
    action.setReferer("");
    if (isValid(fields, 7)) action.setReferer(fields[7]);
    return action;
  }

  private boolean isValid(String[] f, int i) {
    if (f.length > i && f[i] != null) {
      return true;
    }
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      CSVAvroSerializer ser = new CSVAvroSerializer(out);
      ser.configure(context);
      return ser;
    }

  }

}

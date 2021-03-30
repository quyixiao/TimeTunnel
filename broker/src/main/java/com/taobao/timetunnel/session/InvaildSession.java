package com.taobao.timetunnel.session;

import java.nio.ByteBuffer;

import com.taobao.util.Bytes;

/**
 * {@link InvaildSession}
 * 
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 * @created 2010-12-31
 * 
 */
public final class InvaildSession implements Session {
  public InvaildSession(final ByteBuffer token) {
    id = Bytes.toString(token);
  }

  @Override
  public void add(final InvalidListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean booleanValueOf(final Attribute attribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public int intValueOf(final Attribute attribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInvalid() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long longValueOf(final Attribute attribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void remove(final InvalidListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String stringValueOf(final Attribute attribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type type() {
    return Type.pub;
  }

  private final String id;

}

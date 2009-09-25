/*
  Copyright 2009 RapLeaf

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

public class RapBitSet {
  private static final byte[] bitvalues = new byte[] {
    (byte)0x01,
    (byte)0x02,
    (byte)0x04,
    (byte)0x08,
    (byte)0x10,
    (byte)0x20,
    (byte)0x40,
    (byte)0x80
  };
  
  private byte[] _bytes;
  private int _numBits;
  
  public RapBitSet(int numBits) {
    _numBits = numBits;
    _bytes = new byte[getNBytes()];
  }
  
  private int getNBytes() {
    return (_numBits+7)/8;
  }
  
  public RapBitSet(int numBits, byte[] arr) {
    _bytes = arr;
    _numBits = numBits;
  }
  
  public int numBits() {
    return _numBits;
  }
  
  public byte[] getRaw() {
    return _bytes;
  }
  
  public void clear() {
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = 0;
    }
  }
  
  public void fill() {
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = (byte) 0xFF;
    }
  }
  
  public boolean get(int pos) {
    int byteNum = pos/8;
    int bytePos = pos%8;
    return (_bytes[byteNum] & bitvalues[bytePos]) != 0;
  }
  
  public void set(int pos) {
    int byteNum = pos/8;
    int bytePos = pos%8;
    _bytes[byteNum] = (byte) (_bytes[byteNum] | bitvalues[bytePos]);
  }
  
  public void unset(int pos) {
    int byteNum = pos/8;
    int bytePos = pos%8;
    _bytes[byteNum] = (byte) (_bytes[byteNum] ^ bitvalues[bytePos]);
  }
  
  public void flip() {
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = (byte) ~_bytes[i];
    }
  }
  
  public void or(RapBitSet other) {
    if(other.numBits()!=numBits()) throw new IllegalArgumentException("Must be same size sets");
    byte[] otherBytes = other._bytes;
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = (byte) (_bytes[i] | otherBytes[i]);
    }
  }
  
  public void and(RapBitSet other) {
    if(other.numBits()!=numBits()) throw new IllegalArgumentException("Must be same size sets");
    byte[] otherBytes = other._bytes;
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = (byte) (_bytes[i] & otherBytes[i]);
    }
  }
  
  public void xor(RapBitSet other) {
    if(other.numBits()!=numBits()) throw new IllegalArgumentException("Must be same size sets");
    byte[] otherBytes = other._bytes;
    for(int i=0; i<_bytes.length; i++) {
      _bytes[i] = (byte) (_bytes[i] ^ otherBytes[i]);
    }
  }
  
  
}
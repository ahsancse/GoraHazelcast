/*
 Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main.java.org.apache.gora.hazelcast.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BinaryEncoder implements Encoder {
    public byte[] encodeShort(short s) {
        return encodeShort(s, new byte[2]);
    }

    public byte[] encodeShort(short s, byte ret[]) {                                  // encode Short values into byte
        try{
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeShort(s);
            return ret;
        } catch (IOException ioe) {
              throw new RuntimeException(ioe);
        }
    }

    public short decodeShort(byte[] a) {                                            // decode byte into short
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            short s = dis.readShort();
            return s;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeInt(int i) {                                             // encode int values into byte
        return encodeInt(i, new byte[4]);
    }

    public byte[] encodeInt(int i, byte ret[]) {
        try {
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeInt(i);
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public int decodeInt(byte[] a) {                                              // decode byte into int
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            int i = dis.readInt();
            return i;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeLong(long l) {                                           // encode long values into byte
        return encodeLong(l, new byte[8]);
    }

    public byte[] encodeLong(long l, byte ret[]) {
        try {
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeLong(l);
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public long decodeLong(byte[] a) {                                                  // decode byte into long
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            long l = dis.readLong();
            return l;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeDouble(double d) {                                          // encode double values into byte
        return encodeDouble(d, new byte[8]);
    }

    public byte[] encodeDouble(double d, byte[] ret) {
        try {
            long l = Double.doubleToRawLongBits(d);
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeLong(l);
           return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public double decodeDouble(byte[] a) {                                        // decode byte into double
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            long l = dis.readLong();
            return Double.longBitsToDouble(l);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeFloat(float d) {                                            // encode float values into byte
        return encodeFloat(d, new byte[4]);
    }

    public byte[] encodeFloat(float f, byte[] ret) {
        try {
            int i = Float.floatToRawIntBits(f);
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeInt(i);
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public float decodeFloat(byte[] a) {                                             // decode byte into float
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            int i = dis.readInt();
            return Float.intBitsToFloat(i);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeByte(byte b, byte[] ret) {
        ret[0] = 0;
        return ret;
    }

    public byte[] encodeByte(byte b) {
        return encodeByte(b, new byte[1]);
    }

    public byte decodeByte(byte[] a) {
        return a[0];
    }

    public boolean decodeBoolean(byte[] a) {                                            // decode byte into boolean
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a));
            return dis.readBoolean();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] encodeBoolean(boolean b) {                                                   // encode boolean values into byte
        return encodeBoolean(b, new byte[1]);
    }

    public byte[] encodeBoolean(boolean b, byte[] ret) {
        try {
            DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));
            dos.writeBoolean(b);
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] lastPossibleKey(int size, byte[] er) {
        return Utilitys.lastPossibleKey(size, er);
    }

    public byte[] followingKey(int size, byte[] per) {
        return Utilitys.followingKey(size, per);
    }
}

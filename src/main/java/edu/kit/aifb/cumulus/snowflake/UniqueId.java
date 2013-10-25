package edu.kit.aifb.cumulus.snowflake;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.eaio.uuid.UUIDGen;

public class UniqueId {
    // Get the MAC address (i.e., the "node" from a UUID1)
    private final long clockSeqAndNode = UUIDGen.getClockSeqAndNode();
    private final byte[] node = new byte[]{
        (byte)((clockSeqAndNode >> 40) & 0xff),
        (byte)((clockSeqAndNode >> 32) & 0xff),
        (byte)((clockSeqAndNode >> 24) & 0xff),
        (byte)((clockSeqAndNode >> 16) & 0xff),
        (byte)((clockSeqAndNode >> 8) & 0xff),
        (byte)((clockSeqAndNode >> 0) & 0xff),
    };
    private final ThreadLocal<ByteBuffer> tlbb = new ThreadLocal<ByteBuffer>() {
        @Override
        public ByteBuffer initialValue() {
            return ByteBuffer.allocate(16);
        }
    };

    private volatile int seq;
    private volatile long lastTimestamp;
    private final Object lock = new Object();

    private final int maxShort = (int)0xffff;

    public byte[] getId() {
        if(seq == maxShort) {
            throw new RuntimeException("Too fast");
        }

        long time;
        synchronized(lock) {
            time = System.currentTimeMillis();
            if(time != lastTimestamp) {
                lastTimestamp = time;
                seq = 0;
            }
            seq++;
            ByteBuffer bb = tlbb.get();
            bb.rewind();
            bb.putLong(time);
            bb.put(node);
            bb.putShort((short)seq);
            return bb.array();
        }
    }

    private volatile int seqTEST=0;

    public String getStringId() {
      byte[] ba = getId();
      ByteBuffer bb = ByteBuffer.wrap(ba);
      long ts = bb.getLong();
      int node_0 = bb.getInt();
      short node_1 = bb.getShort();
      short seq = bb.getShort();
      //return String.format("%016d-%s%s-%04d", ts, Integer.toHexString(node_0), Integer.toHexString(node_1), seq);
      String r;
      synchronized(this) {
          r = ts+"-"+String.valueOf(++seqTEST);
      }
      return r;
    }

    public static void main(String[] args) throws IOException {
        UniqueId uid = new UniqueId();
        int n = 10;

        for(int i=0; i<n; i++) {
            //System.out.println(Long.valueOf(new String(uid.getId())));
            System.out.println(uid.getStringId());
        }
    }
}
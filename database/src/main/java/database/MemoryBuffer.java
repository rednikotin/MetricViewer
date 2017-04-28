package database;

import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class MemoryBuffer {

    private static final Unsafe unsafe;
    private static final int BYTE_ARRAY_OFFSET;
    private static final Method mmap;
    private long addr;
    private long size;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
            mmap = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
            mmap.setAccessible(true);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate Unsafe", e);
        }
    }

    MemoryBuffer(long size) {
        this.size = size;
        this.addr = unsafe.allocateMemory(size);
    }

    MemoryBuffer(RandomAccessFile raf, long size)
            throws IllegalAccessException, InvocationTargetException {
        this.size = size;
        this.addr = (long) mmap.invoke(raf.getChannel(), 1, 0L, size);
    }

    public void put(long pos, byte b) {
        unsafe.putByte(addr + pos, b);
    }

    public void put(long pos, byte[] arr) {
        unsafe.copyMemory(arr, BYTE_ARRAY_OFFSET, null, addr + pos, arr.length);
    }

    public void put(long pos, ByteBuffer bb) {
        int n = bb.remaining();
        for (int i = 0; i < n; i++) {
            put(pos + i, bb.get());
        }
    }

    public byte get(long pos) {
        return unsafe.getByte(addr + pos);
    }

    public void get(long pos, byte[] arr) {
        unsafe.copyMemory(null, pos + addr, arr, BYTE_ARRAY_OFFSET, arr.length);
    }

    public long getSize() {
        return this.size;
    }
}

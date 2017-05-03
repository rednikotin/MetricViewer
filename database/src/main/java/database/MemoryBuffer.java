package database;

import sun.misc.Unsafe;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class MemoryBuffer {

    private final static Unsafe unsafe = UnsafeUtil.getUnsafe();
    private long addr;
    private long size;

    MemoryBuffer(long size) {
        this.size = size;
        this.addr = unsafe.allocateMemory(size);
    }

    MemoryBuffer(RandomAccessFile raf, long size)
            throws IllegalAccessException, InvocationTargetException {
        this.size = size;
        this.addr = (long) UnsafeUtil.mmap(raf.getChannel(), size);
    }

    public void put(long pos, byte b) {
        unsafe.putByte(addr + pos, b);
    }

    public void put(long pos, byte[] arr) {
        unsafe.copyMemory(arr, UnsafeUtil.BYTE_ARRAY_OFFSET, null, addr + pos, arr.length);
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
        unsafe.copyMemory(null, pos + addr, arr, UnsafeUtil.BYTE_ARRAY_OFFSET, arr.length);
    }

    public long getSize() {
        return this.size;
    }
}

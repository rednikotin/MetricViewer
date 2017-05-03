package database;

import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.channels.FileChannel;

public class UnsafeUtil {

    private static final Unsafe unsafe;
    private static final Field bufferAddress;
    private static final Method map0;

    public static final int BYTE_ARRAY_OFFSET;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
            map0 = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
            map0.setAccessible(true);
            bufferAddress = Buffer.class.getDeclaredField("address");
            bufferAddress.setAccessible(true);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate Unsafe", e);
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    public static long getBufferAddress(Buffer db) throws IllegalAccessException {
        return (long)bufferAddress.get(db);
    }

    public static long mmap(FileChannel ch, long size) throws IllegalAccessException, InvocationTargetException {
        return (long) map0.invoke(ch, 1, 0L, size);
    }
}

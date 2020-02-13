package com.microsoft;

import java.nio.file.*;
import java.net.URL;
import java.io.File;
import java.util.jar.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// avoid dependencies for javah
public class ASANative {
    public static final String CLR_DIRECTORY = "clr/";
    public static final String DLL_BINDING_DIRECTORY = "binding/";
    public static final String ASANATIVE_TEMP_DIRECTORY = "asanative";
    public static final String LIBASA_SO_FILE = "libasa.so";
    static final Logger logger = Logger.getLogger(ASANative.class);

    private static void ExtractFolder(Path temp) throws Exception {
        File jarFile = new File(ASANative.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        JarFile jar = new JarFile(jarFile);
        Enumeration<JarEntry> entries = jar.entries();
        while(entries.hasMoreElements()) {
            final String name = entries.nextElement().getName();
            if (name.endsWith(FileSystems.getDefault().getSeparator()))
                continue;

            if (name.startsWith(CLR_DIRECTORY) || name.startsWith(DLL_BINDING_DIRECTORY)) {
                Path parent = Paths.get(name).getParent();
                if (parent != null)
                    Files.createDirectories(temp.resolve(parent));

                Files.copy(ASANative.class.getResourceAsStream(FileSystems.getDefault().getSeparator() + name), temp.resolve(name));
            }
        }
        jar.close();
    }

    private static Path tempDirectory;
    private static long clrPtr;

    static
    {
        try {
            tempDirectory = Files.createTempDirectory(ASANATIVE_TEMP_DIRECTORY);
            logger.info("TEMP DIR: " + tempDirectory);

            ExtractFolder(tempDirectory);

            System.load(tempDirectory.resolve(DLL_BINDING_DIRECTORY + LIBASA_SO_FILE).toString());

            clrPtr = startCLR(tempDirectory.toString());
    
        } catch (Exception e) {
            //TODO: handle exception
            logger.debug("Unable to extract: " + e.getMessage());
        }
    }

    //ASANative destructor: clean up temp directory created in constructor
    public static void deleteTemp()
    {
        try {
            deleteDirectory(tempDirectory.toFile());
        }
        catch (Exception e)
        {
            logger.debug("Unable to clean up temp directory: " + e.getMessage());
        }
    }

    private static boolean deleteDirectory(File toDelete)
    {
        File[] allContents = toDelete.listFiles();
        if (allContents != null)
        {
            for (File file : allContents) 
            {
                deleteDirectory(file);
            }
        }
        return toDelete.delete();
    }

    public static native long startCLR(String tempDir);

    public static long startASA(String sql)
    {
        try
        {
            return startASA(clrPtr, tempDirectory.toString(), sql);
        }
        catch (Exception e)
        {
            deleteTemp();
            throw e;
        }
    }

    public static void stopAndClean(long ptr)
    {
        stopASA(ptr);
        deleteTemp();
    }

    private static native long startASA(long clrPtr, String tempDir, String sql);
    private static native void stopASA(long ptr);

    public static void getOutputSchema(String sql, long clrPtr)
    {
        try
        {
            getOutputSchema(clrPtr, sql);
        }
        catch (Exception e)
        {
            deleteTemp();
            throw e;
        }
    }
    
    private static native void getOutputSchema(long clrPtr, String sql);

    public static void registerArrowMemory(ByteBuffer outputBuffer, int outputBufferLength,
            ByteBuffer inputBuffer, int inputBufferLength, long ptr)
    {
        try
        {
            registerArrowMemory(ptr, outputBuffer, outputBufferLength, inputBuffer, inputBufferLength);
        }
        catch (Exception e)
        {
            deleteTemp();
            throw e;
        }
    }
    
    private static native void registerArrowMemory(long ptr, ByteBuffer outputBuffer, int outputBufferLength,
            ByteBuffer inputBuffer, int inputBufferLength);

    // TODO: this should be column batched to avoid massive amount of calls.
    // re-use memory on the java-side and pin on C++ for memory copy?

    public static native void nextOutputRecord(long ptr);

    // returns rows discovered in output.
    public static native int pushRecord(long ptr);

    // no more data to come
    public static native int pushComplete(long ptr);
}

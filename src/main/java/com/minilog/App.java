package com.minilog;
import java.nio.file.Path;

import com.minilog.log.LogSegment;
import com.minilog.log.Record;


public class App 
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        var seg = new LogSegment(0, Path.of("data/topic0"));
        // long offset1 = seg.append(new Record("k1".getBytes(), "hello".getBytes()));
        // long offset2 = seg.append(new Record("k2".getBytes(), "World".getBytes()));
        
        // System.out.println("Appended records at offsets: " + offset1 + ", " + offset2);
        Record rec1 = seg.read(0);
        Record rec2 = seg.read(1);

        System.out.println(new String(rec1.key()) + " = " + new String(rec1.value()));
        System.out.println(new String(rec2.key()) + " = " + new String(rec2.value()));

    }
}

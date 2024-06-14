package com.seqera;

import java.io.Closeable;
import java.util.Iterator;

public interface DataSource extends Iterator<InputRecord>, Closeable {}

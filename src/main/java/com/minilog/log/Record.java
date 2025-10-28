package com.minilog.log;

/** Immutable holder for a message key/value pair. */
public record Record(byte[] key, byte[] value) {}

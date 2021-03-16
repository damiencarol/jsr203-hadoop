jsr203-hadoop
=============

JSR 203 Implementation for Hadoop Distributed File System

[![Build Status](https://travis-ci.org/damiencarol/jsr203-hadoop.svg?branch=master)](https://travis-ci.org/damiencarol/jsr203-hadoop)
[![Coverage Status](https://coveralls.io/repos/damiencarol/jsr203-hadoop/badge.svg?branch=master&service=github)](https://coveralls.io/github/damiencarol/jsr203-hadoop?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jsr203hadoop/jsr203hadoop/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.jsr203hadoop/jsr203hadoop/)
[![Dependency Status](https://www.versioneye.com/user/projects/56c2fdbc18b2710036c8d4af/badge.svg)](https://www.versioneye.com/user/projects/56c2fdbc18b2710036c8d4af)
[![Codacy Badge](https://api.codacy.com/project/badge/grade/24aa7d876cac493e89512b88df3887ab)](https://www.codacy.com/app/damien-carol/jsr203-hadoop)
[![](https://jitpack.io/v/damiencarol/jsr203-hadoop.svg)](https://jitpack.io/#damiencarol/jsr203-hadoop)

#### Maven artifact

```xml
    <dependency>
        <groupId>com.github.jsr203hadoop</groupId>
        <artifactId>jsr203hadoop</artifactId>
        <version>1.0.3</version>
    </dependency>
```

#### Prerequisites

`jsr203-hadoop` requires:
* Git
* Maven 3.0.4 (or higher)
* JDK 1.7 (or higher)

#### Download and build

```bash
$ git clone git://github.com/damiencarol/jsr203-hadoop.git
$ cd jsr203-hadoop
$ mvn install
```

Use `mvn -DskipTests` if you do not want to execute the tests.

#### Example

`jsr203-hadoop` provide NIO 2 access to your HDFS cluster. For
example, you can build URI starts with `hdfs://<namebode>:<port>` and use standard NIO API :

```java
URI uri = new URI("hdfs://" + host + ":" + port + "/somefile");
Path file = Paths.get(uri);
System.out.println("File [" + file.toString() + "] exists = '" + Files.exists(file) + "'");
```


#### Status

This project is still a beta.

The following features are complete.

| Feature | Status | Description |
|---------|--------|-------------|
| Path support - build from string/URI | Done ||
| Path support - resolve, relativize, equals, ... | Done |
| Directory stream (provide ability to list files in directory, get status, ...) | Done ||
| Create/delete file/directory | Done ||
| Input/Output | Partial | Full support of SeekableByteChannel, FileChannel pending | 
| Attribute view "basic" | Done | Full support (fileKey is implemented) |
| Attribute view "owner" | Done | Full support (Use Hadoop UGI) |
| Attribute view "posix" | Done ||
| Hadoop dedicated view "hadoop" for hadoop specific attributes (len, block size, replication factor) | Done | More attributes will come |

More features will come :

* Security and permissions
* complete list of hadoop specific attributes

For more details, see the <a href="REFERENCE.md">Reference guide</a>.

#### More information

* License: <a href="LICENSE">Apache License, Version 2.0.</a>
* Author: <a href="https://github.com/damiencarol">Damien Carol</a>
* Source code: <a href="http://github.com/damiencarol/jsr203-hadoop">http://github.com/damiencarol/jsr203-hadoop</a>

#### Ressources

* <a href="https://jcp.org/en/jsr/detail?id=203">JSR 203 NIO 2 spec</a>
* <a href="http://docs.oracle.com/javase/tutorial/essential/io/fileio.html">Tutorial about NIO 2 from Oracle</a>

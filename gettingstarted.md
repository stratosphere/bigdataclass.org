--- 
layout: with_comment 
title:  "Getting Started"
---

#Getting Started

First, you need to install git, Maven 3 and Eclipse on your machine.

```
sudo apt-get install git maven eclipse
```

Next, clone the skeleton for the exercises:

```
git clone https://github.com/stratosphere/bigdataclass.org.git
cd bigdataclass.org
```

Inside the folder there is another folder for each exercise. Right now
there is `exercise1` which contains the java exercise and `exercise2` which
contains the Scala exercise. Each exercise is a self-contained maven
project, you can import this project into Eclipse,
using the "Import -> Import as Maven Project" menu.
This can take a while as Maven is going to download all the dependencies.

When you are working on the Scala exercise you will also need the following
plugins:

Eclipse 4.x:

  * scala-ide: http://download.scala-ide.org/sdk/e38/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.15.0/N/0.15.0.201206251206/

Eclipse 3.7:

  * scala-ide: http://download.scala-ide.org/sdk/e37/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/

#FAQ

**I'm getting a *OutOfMemoryException* on Mac OS X**

Its seems that OS X does not allocate enough memory for Stratosphere's `LocalExecutor`.
Open the Run Configuration (the drop down menu right to the "Run" button) and add the following to the JVM Arguments `-Xms400m -Xmx800m`.






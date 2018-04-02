# ODE Feature Engine [![Build Status](https://travis-ci.org/Project-ODE/FeatureEngine.svg?branch=master)](https://travis-ci.org/Project-ODE/FeatureEngine)

Spark jobs computing features from raw data and loading them into ES.

## Pre-requisites

Make sure you have java 8, scala and sbt installed.

### Debian / Unbuntu

```sh
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install openjdk-8-jdk scala
```

And for sbt: http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html

## Usage

In the *FeatureEngine* directory, start sbt, then compile and test:

```sh
sbt
compile
test
```

First run of sbt might be long as it will download all needed dependencies.

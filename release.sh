#!/bin/bash

KHRONUS_HOME=$(cd "`dirname "$0"`"; pwd)
sbt clean universal:packageZipTarball
ls -l  $KHRONUS_HOME/khronus/target/universal/khronus*.tgz 


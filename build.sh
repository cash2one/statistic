#!/bin/bash
tar -czvf kgdc-statist.tar.gz --exclude=.* *
mkdir output
mv kgdc-statist.tar.gz output

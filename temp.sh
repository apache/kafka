#!/bin/bash

cat temp | awk -F":" '{print $1}' > temp2
cat temp2 | while read in; do git add "$1"; done

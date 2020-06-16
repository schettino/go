#!/bin/bash

docker ps  |  sed -n '2 p' | awk '{print "Runtime: " $9 " " $10}'
echo
echo
echo "Currently running subranges:"
echo

I=1
for S in $(ps aux | grep /usr/local/bin/stellar-core | grep catchup |  awk '{ print $15 }' | sort -n ); do
    printf '%15s' $S
    if [ $(( I %  5 )) = 0 ]; then
	echo
    fi
    I=$(( I + 1))
done


#!/bin/sh
# Start a real 3-node cluster on localhost, write to it, kill the leader,
# and show that the data survives and the cluster keeps serving.
set -e
C=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
D=$(mktemp -d)
trap '{ kill $P0 $P1 $P2; wait; } 2>/dev/null; rm -rf "$D"' EXIT
python3 -m raftsim serve --id 0 --cluster $C --data "$D/0" > "$D/0.log" & P0=$!
python3 -m raftsim serve --id 1 --cluster $C --data "$D/1" > "$D/1.log" & P1=$!
python3 -m raftsim serve --id 2 --cluster $C --data "$D/2" > "$D/2.log" & P2=$!
sleep 1
echo "put city Shillong  -> $(python3 -m raftsim client --cluster $C put city Shillong)"
echo "append city ', IN' -> $(python3 -m raftsim client --cluster $C append city ', IN')"
echo "get city           -> $(python3 -m raftsim client --cluster $C get city)"
python3 -m raftsim client --cluster $C bench ${1:-3000} 16
for i in 0 1 2; do tail -1 "$D/$i.log" | grep -q "leader in term" && L=$i; done
echo "killing leader (node $L)"
eval "{ kill -9 \$P$L; wait \$P$L; } 2>/dev/null" || true
echo "get city           -> $(python3 -m raftsim client --cluster $C get city)"
echo "cas city           -> $(python3 -m raftsim client --cluster $C cas city 'Shillong, IN' 'Shillong, Meghalaya')"
echo "writing ${1:-3000} more entries while node $L is down (the survivors compact their logs)"
python3 -m raftsim client --cluster $C bench ${1:-3000} 16 > /dev/null
echo "restarting node $L: its log is now behind the survivors' snapshots"
python3 -m raftsim serve --id $L --cluster $C --data "$D/$L" > "$D/$L.log" & eval P$L=$!
sleep 2
echo "  before: $(head -1 "$D/$L.log")"
eval "{ kill \$P$L; wait \$P$L; } 2>/dev/null" || true
python3 -m raftsim serve --id $L --cluster $C --data "$D/$L" > "$D/$L.log" & eval P$L=$!
sleep 1
echo "  after:  $(head -1 "$D/$L.log")"
echo "get city           -> $(python3 -m raftsim client --cluster $C get city)"
echo "log file sizes: $(wc -c < "$D/0/log.jsonl") $(wc -c < "$D/1/log.jsonl") $(wc -c < "$D/2/log.jsonl") bytes"

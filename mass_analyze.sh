#!/usr/bin/env bash

for input in ./inputs/*.json.gz; do
	filename=$(basename -- "$input")
	python pca_analyzer/generate_mission_hit_log.py $input > ./hit_logs/"$filename.csv" &
done
wait
python pca_analyzer/generate_name_weapon.py ./hit_logs/ &
wait

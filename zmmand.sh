# mbf smoke test: load 25GB with full / partitioned / mbf filters, then read with perf context metadata collection
./load_full_part_mbf.sh
DB_BASE_DIR=/work/mbftest/load_25gb_full_part_mbf_20260303_035608/ bash read_full_part_mbf.sh




./bg_full_cache.sh /work/background/500gb_full_filter 500gb

python3 parse_perf_context_level_metadata.py

python3 plot_perf_context_hit_level.py --db-size=250GB

python3 summarize_filter_index_access.py
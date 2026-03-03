./bg_full_cache.sh /work/background/500gb_full_filter 500gb

python3 parse_perf_context_level_metadata.py

python3 plot_perf_context_hit_level.py --db-size=250GB

python3 summarize_filter_index_access.py
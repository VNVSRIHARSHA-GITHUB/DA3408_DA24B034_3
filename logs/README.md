# Log file mapping (Ray)
Counted runs (reported in the report and readme):
- Run 1 (149.0 s): baseline_run2_controlled.txt, oom_check_run2_controlled.txt, top_ray_run2_controlled.log
- Run 2 (147.8 s): baseline_run2_official.txt, oom_check_run2_official.txt, top_ray_run2_official.log
- Run 3 (146.7 s): baseline_run3_official.txt, oom_check_run3_official.txt, top_ray_run3_official.log
Development runs (not counted):
- top_ray_run1.log, oom_check_run1.txt: 230.8 s, with the shuffle option that was later removed
- top_ray_run2_attempt1.log, oom_check_run2.txt: 159.4 s, workers killed (memory pressure)
- mem_fb4.log: memory log from the 3-month test of that option


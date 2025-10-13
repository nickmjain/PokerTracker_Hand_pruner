PokerTracker 4 Database Pruner

Overview
This utility provides an optimized, set-based solution for reducing the physical size of a PokerTracker 4 database. It executes surgical hand deletion, focusing exclusively on historical hands where 100% of participating players have been inactive for a defined period (N days), so as to remove data you no longer need to track, i.e., for players who you are no longer likely to encounter ever again, since they have not been playing for over a year.

The approach prioritizes data integrity and performance. Some intentional under-performance design decisions have been (e.g., batching) so as to provide more analytical visibility (i.e., determine pruning ratio over evolving time windows to show older hands are more likely to be pruned than newer hands)

Global Player Retention: Data for any player active within the threshold (N days) is 100% preserved, regardless of when the hands were played.

Bulk Efficiency: Uses large, single-query SQL operations via CTEs to achieve superior performance compared to iterative scripting.

Safety First: Defaults to Dry Run mode, requiring explicit confirmation (--commit) for actual deletion.

Technical Core
The pruning logic relies on identifying a definitive set of hands for deletion:

A hand's date must be older than the set threshold.

AND The hand's players must not exist in the global_active_players CTE (which contains all players active within the last N days).

A maximum limit is applied to the resulting set to ensure transaction manageability.

Performance Optimizations
For very large databases (50M+ hands, 200GB+), the script includes advanced performance features:

1. Chunked Processing with Multiprocessing:
   - Divides work by date ranges for natural data partitioning
   - Processes chunks in parallel using Python's multiprocessing
   - Enable with `--parallel` and configure with `--chunks` and `--workers`

2. PostgreSQL Parallel Query Support:
   - Enables PostgreSQL internal parallel query execution
   - Optimizes server settings for bulk operations
   - Enable with `--pg-parallel` and configure with `--pg-workers`, `--pg-work-mem`, and `--pg-maintenance-mem`

3. Two-Phase Processing with Temp Tables:
   - Creates temporary indexed tables for eligible hands
   - Avoids re-executing complex queries for each deletion
   - Enable with `--two-phase`

Post-Pruning Maintenance
To fully reclaim disk space and optimize query paths after a successful --commit, the following maintenance commands are required in your PostgreSQL console:

VACUUM FULL;
REINDEX DATABASE [db_name];

Author & Version Information
Field

Value

### Author Info:
Nick Jain 
October 9, 2025
https://nickjain.com
https://www.linkedin.com/in/nickmjain/
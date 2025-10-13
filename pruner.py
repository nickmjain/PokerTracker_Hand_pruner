#!/usr/bin/env python3
"""
PT4 Database Pruner

This script surgically removes poker hands from a PokerTracker 4 database where
ALL players in the hand have been inactive for a specified period (N days).

It now executes the pruning check in small, sequential batches (using LIMIT/OFFSET) 
to provide granular progress tracking across the full analysis limit, prioritizing 
analytical insight over maximum runtime efficiency.

NOTE: Database credentials (HOST, NAME, USER, PASSWORD) MUST be provided
via the .env file and cannot be overridden by command-line arguments.
"""

import argparse
import datetime
import os
import sys
import time
import math
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration & Utility Functions ---

def parse_arguments():
    """Parse command line arguments and environment variable fallbacks."""
    parser = argparse.ArgumentParser(
        description="Bulk prune inactive hands from PokerTracker 4 database via optimized SQL. Runs in dry-run mode by default."
    )
    
    # --- Database Credentials (Read ONLY from .env) ---
    db_args = {
        'host': os.getenv("DB_HOST"),
        'dbname': os.getenv("DB_NAME"),
        'user': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"),
    }
    
    # --- Pruning Parameters (CLI overrides allowed) ---
    parser.add_argument("--days", type=int, default=int(os.getenv("DEFAULT_DAYS", 365)), 
                        help="Inactivity threshold in days (N).")
    
    # New: Use --commit to override the dry-run default (dry_run=True).
    parser.add_argument("--commit", action="store_true", 
                        help="Enable real deletion mode. By default, the script runs in dry-run mode.")
    
    # New: Limit the number of hands processed. Default is 1,000,000.
    parser.add_argument("--limit", type=int, default=int(os.getenv("HAND_LIMIT", 1000000)), 
                        help="Maximum number of eligible hands to process (default: 1,000,000).")

    # New: Single argument for hand type. Default is 'cash'.
    parser.add_argument("--type", choices=['cash', 'tourney', 'both'], default=os.getenv("HAND_TYPE", "cash"),
                        help="Select hand type(s) to process: cash, tourney, or both (default: cash).")


    args = parser.parse_args()
    
    # Merge DB credentials into the args Namespace
    for key, value in db_args.items():
        setattr(args, key, value)

    # Runtime Validation: Check mandatory DB fields from .env
    required_db_fields = ['host', 'dbname', 'user', 'password']
    missing_fields = [f for f in required_db_fields if not getattr(args, f)]
    
    if missing_fields:
        parser.error(f"Missing required database credentials in .env: {', '.join(f.upper() for f in missing_fields)}")

    if args.days <= 0:
        parser.error("Days must be a positive integer.")
    if args.limit <= 0:
        parser.error("Limit must be a positive integer.")

    # Determine hand types based on --type argument
    if args.type == 'cash':
        args.hand_types = ['cash']
    elif args.type == 'tourney':
        args.hand_types = ['tourney']
    else: # 'both'
        args.hand_types = ['cash', 'tourney']
        
    # Set dry_run state based on --commit
    args.dry_run = not args.commit

    return args


def get_table_names(hand_type):
    """Maps hand type to PT4 table names."""
    if hand_type == 'cash':
        return {
            'summary': sql.Identifier('cash_hand_summary'),
            'player_stats': sql.Identifier('cash_hand_player_statistics')
        }
    elif hand_type == 'tourney':
        return {
            'summary': sql.Identifier('tourney_hand_summary'),
            'player_stats': sql.Identifier('tourney_hand_player_statistics')
        }
    raise ValueError(f"Unknown hand type: {hand_type}")


def create_connection(args):
    """Establishes database connection."""
    try:
        conn = psycopg2.connect(
            host=args.host, dbname=args.dbname, user=args.user, password=args.password
        )
        # We still need autocommit=False for the final COMMIT/ROLLBACK logic
        conn.autocommit = False 
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

# --- Core Pruning Logic (SQL Batch Operations) ---

def execute_bulk_pruning(conn, cursor, hand_type, days, cutoff_date, dbname, dry_run=False, hand_limit=1000000):
    """
    Executes the pruning check sequentially using LIMIT and OFFSET to provide 
    granular progress tracking per 100k hand window.
    """
    tables = get_table_names(hand_type)
    
    print(f"\n[{hand_type.upper()}] Running sequential batch prune operation...")
    
    # CTE to identify ALL active players (calculated once, used repeatedly)
    active_player_cte = sql.SQL("""
        WITH global_active_players AS (
            -- Active players from cash
            SELECT DISTINCT id_player
            FROM cash_hand_player_statistics chps_c
            JOIN cash_hand_summary chs_c ON chps_c.id_hand = chs_c.id_hand
            WHERE chs_c.date_played > (CURRENT_DATE - INTERVAL %s)
            UNION
            -- Active players from tourney
            SELECT DISTINCT id_player
            FROM tourney_hand_player_statistics thps_t
            JOIN tourney_hand_summary ths_t ON thps_t.id_hand = ths_t.id_hand
            WHERE ths_t.date_played > (CURRENT_DATE - INTERVAL %s)
        )
    """)
    
    # 1. Logic to count active players (Initial phase, using the main cursor)
    cte_params = [f"{days} days", f"{days} days"]
    count_active_players_query = sql.SQL("""
        {active_cte}
        SELECT COUNT(id_player)
        FROM global_active_players;
    """).format(active_cte=active_player_cte)

    print(f"Pre-scan: Identifying all active players to retain hands for...")
    cursor.execute(count_active_players_query, cte_params)
    active_player_count = cursor.fetchone()[0]
    print(f"[INFO] Found {active_player_count:,} unique active players.")

    
    # --- Execute Pruning in Sequential Batches (LIMIT/OFFSET) ---
    BATCH_SIZE = 500000 # Fixed batch size for analysis window
    total_input_batches = math.ceil(hand_limit / BATCH_SIZE)
    
    pruned_hand_ids = []
    
    print(f"Starting granular analysis loop (Total windows to analyze: {total_input_batches:,})")
    print(f"--- Each window processes {BATCH_SIZE:,} hands from oldest to newest ---")
    
    cumulative_pruned_count = 0
    total_analyzed = 0
    
    # 2. Main Iteration Loop
    for batch_number in range(total_input_batches):
        offset = batch_number * BATCH_SIZE
        
        # Determine the number of hands to analyze in this specific batch (handles the final partial batch)
        current_limit = min(BATCH_SIZE, hand_limit - offset)
        if current_limit <= 0:
            break # Should be caught by total_input_batches, but defensive break
            
        start_time_batch = time.time()

        # SQL Query to identify hands to delete for the CURRENT 100K window
        hands_to_prune_batch_query = sql.SQL("""
            {active_cte}
            , current_batch_candidates AS (
                -- Select the current 100k hands (by date) to analyze
                SELECT {summary}.id_hand
                FROM {summary}
                WHERE {summary}.date_played <= %s  -- Cutoff Date
                ORDER BY {summary}.date_played ASC, {summary}.id_hand ASC
                LIMIT %s   -- Current LIMIT (usually 100k)
                OFFSET %s  -- Current OFFSET
            )
            SELECT cbc.id_hand
            FROM current_batch_candidates cbc
            WHERE NOT EXISTS (
                -- Expensive check: is any player in this batch's hand in the global_active_players list?
                SELECT 1
                FROM {player_stats}
                JOIN global_active_players gap ON {player_stats}.id_player = gap.id_player
                WHERE {player_stats}.id_hand = cbc.id_hand
            )
        """).format(
            active_cte=active_player_cte,
            summary=tables['summary'],
            player_stats=tables['player_stats']
        )
        
        # Parameters: [days, days, cutoff_date, current_limit, offset]
        params = [f"{days} days", f"{days} days", cutoff_date, current_limit, offset]
        
        # Execute the query for the current batch
        try:
            cursor.execute(hands_to_prune_batch_query, params)
            batch_results = cursor.fetchall()
        except psycopg2.Error as e:
            print(f"\nCRITICAL QUERY ERROR IN BATCH {batch_number + 1}: {e}")
            break

        current_batch_ids = [row[0] for row in batch_results]
        pruned_hand_ids.extend(current_batch_ids)
        
        batch_pruned_count = len(current_batch_ids)
        cumulative_pruned_count = len(pruned_hand_ids)
        
        # Update total hands analyzed (should equal offset + current_limit)
        total_analyzed = offset + current_limit

        # Calculate the display window
        input_window_start = offset + 1
        input_window_end = total_analyzed

        # --- NEW PROGRESS REPORTING (True Iteration) ---
        elapsed_batch = time.time() - start_time_batch
        print(
            f"Window {batch_number + 1:,} / {total_input_batches:,} (Hands {input_window_start:,}-{input_window_end:,}): "
            f"Pruned this window: {batch_pruned_count:,} | Cumulative Pruned: {cumulative_pruned_count:,} (Time: {elapsed_batch:.2f}s)"
        )

        # The previous check `if len(batch_results) < current_limit: break` incorrectly caused the script to stop
        # whenever the pruning ratio was less than 100%, which is not the desired behavior for chronological analysis.
        # We rely on the total_input_batches loop limit to control the number of hands analyzed (--limit).
            
    hands_to_delete_count = len(pruned_hand_ids)
    
    # --- FINAL SUMMARY ---
    print(f"\n[{hand_type.upper()}] Scan complete.")
    # Report the actual total analyzed, which might be less than 'hand_limit' if we broke early
    print(f"Total hands analyzed by the database: {total_analyzed:,}")
    print(f"Hands found eligible for deletion: {hands_to_delete_count:,}")
    
    # Calculate and print the pruning ratio as a percentage
    if total_analyzed > 0:
        pruning_ratio_percent = (hands_to_delete_count / total_analyzed) * 100
        # The correct format specifier is ':.1f%' to show one decimal place and the percent sign
        print(f"Total pruning ratio: {pruning_ratio_percent:.1f}%")
    else:
        print("Total pruning ratio: N/A (No hands analyzed)")

    # --- END FINAL SUMMARY ---

    print(f"\n[{hand_type.upper()}] Total eligible hands for deletion: {hands_to_delete_count:,} (Dry Run: {dry_run}).")

    if hands_to_delete_count == 0:
        return 0, 0
    
    if dry_run:
        return hands_to_delete_count, 0 # Return 0 deleted rows in dry run

    # B. Actual Deletion (If not dry run) - Using a Temporary Table for Scalability

    temp_table_name = sql.Identifier(f'temp_prune_ids_{hand_type}_{int(time.time())}') # Unique name for safety
    
    print(f"Loading {hands_to_delete_count:,} hand IDs into temporary table for deletion...")
    
    # 1. Create temp table (Postgres handles ON COMMIT DROP automatically)
    cursor.execute(sql.SQL("CREATE TEMP TABLE {} (id_hand BIGINT PRIMARY KEY) ON COMMIT DROP").format(temp_table_name))
    
    # 2. Insert IDs (use `executemany` for speed)
    data_to_insert = [(id_hand,) for id_hand in pruned_hand_ids]
    insert_query = sql.SQL("INSERT INTO {} (id_hand) VALUES (%s)").format(temp_table_name)
    cursor.executemany(insert_query, data_to_insert)
    
    # 3. Perform Deletion using the Temp Table (Fastest way to delete a large list of IDs)

    # Delete from Player Statistics (Child Table)
    delete_stats_query = sql.SQL("""
        DELETE FROM {player_stats}
        WHERE id_hand IN (SELECT id_hand FROM {temp_table})
    """).format(player_stats=tables['player_stats'], temp_table=temp_table_name)
    
    print("Executing DELETE on Player Statistics table...")
    cursor.execute(delete_stats_query)
    stats_deleted = cursor.rowcount
    
    # Delete from Hand Summary (Parent Table)
    delete_summary_query = sql.SQL("""
        DELETE FROM {summary}
        WHERE id_hand IN (SELECT id_hand FROM {temp_table})
    """).format(summary=tables['summary'], temp_table=temp_table_name)

    print("Executing DELETE on Hand Summary table...")
    cursor.execute(delete_summary_query)
    summary_deleted = cursor.rowcount

    # Safety check: summary_deleted should equal hands_to_delete_count
    if summary_deleted != hands_to_delete_count:
        print(f"WARNING: Summary rows deleted ({summary_deleted:,}) does not match pre-scan count ({hands_to_delete_count:,}).")
    
    return summary_deleted, stats_deleted


def main():
    """Entry point for the pruning script."""
    args = parse_arguments()

    print("\n--- PT4 Database Pruner ---")
    print(f"Inactivity Threshold (N): {args.days} days")
    print(f"Dry Run Mode: {args.dry_run} (Use --commit flag for real deletions)")
    print(f"Hand Types: {', '.join(args.hand_types).upper()}")
    print(f"Processing Limit: {args.limit:,} hands per type.\n")

    # Connect to database
    conn = create_connection(args)
    cursor = conn.cursor()

    total_pruned_summary = 0
    start_time = time.time() # Moved start_time here to include connection time in total report
    
    try:
        # Calculate cutoff date
        cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=args.days)).strftime('%Y-%m-%d')
        print(f"Cutoff Date: {cutoff_date} (Hands older than this are eligible for pruning)")
        
        # Process each requested hand type (cash/tourney)
        for hand_type in args.hand_types:
            summary_table = get_table_names(hand_type)['summary']
            
            # Count total hands in the summary table
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(summary_table))
            total_hands = cursor.fetchone()[0]
            print(f"\n[{hand_type.upper()}] Total hands in database: {total_hands:,}")

            # Count hands that will be skipped (not pruned)
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE date_played > %s").format(summary_table), (cutoff_date,))
            skipped_hands = cursor.fetchone()[0]
            print(f"[{hand_type.upper()}] Hands newer than cutoff date (retained): {skipped_hands:,}")
            
            # Execute the heavy lifting via SQL
            # Pass 'conn' now, as it's required for the server-side named cursor in execute_bulk_pruning
            summary_deleted, stats_deleted = execute_bulk_pruning(
                conn, cursor, hand_type, args.days, cutoff_date, args.dbname, args.dry_run, args.limit
            )
            
            total_pruned_summary += summary_deleted
            
            print(f"[{hand_type.upper()}] Result:")
            print(f"  Deleted Hands (Summary): {summary_deleted:,}")
            print(f"  Deleted Rows (Statistics): {stats_deleted:,}")


        # Final Commit or Rollback
        if not args.dry_run and total_pruned_summary > 0:
            print("\nCommit initiated. Persisting changes to database...")
            conn.commit()
            print(f"**Successfully deleted {total_pruned_summary:,} hands in total.**")
            
            # Print maintenance commands per requirement
            print("\n--- ACTION REQUIRED ---")
            print("To fully reclaim the disk space and improve query performance, execute these commands manually:")
            print(f"VACUUM FULL;")
            print(f"REINDEX DATABASE {args.dbname};")
        elif total_pruned_summary > 0:
            print(f"\nDry run successful. {total_pruned_summary:,} hands were eligible for deletion. Changes were NOT committed.")
            conn.rollback()
        else:
            print("\nNo hands were found eligible for deletion. Database remains unchanged.")
            conn.rollback()


    except psycopg2.Error as e:
        print(f"\nCRITICAL DB ERROR: Rolling back transaction.")
        print(e)
        conn.rollback()
    except Exception as e:
        print(f"\nCRITICAL SCRIPT ERROR: {e}")
        conn.rollback()
    finally:
        elapsed_time = time.time() - start_time
        print(f"\n--- Pruner Complete ---")
        print(f"Total time elapsed: {elapsed_time:.2f} seconds.")
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

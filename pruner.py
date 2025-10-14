#!/usr/bin/env python3
"""
PT4 Database Pruner
... [Docstring remains the same] ...
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

# --- Configuration & Utility Functions (No changes needed here) ---

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Bulk prune inactive hands from PokerTracker 4 database via optimized SQL. Runs in dry-run mode by default."
    )

    db_args = {
        'host': os.getenv("DB_HOST"),
        'dbname': os.getenv("DB_NAME"),
        'user': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"),
    }

    parser.add_argument("--days", type=int, default=int(os.getenv("DEFAULT_DAYS", 365)),
                        help="Inactivity threshold in days (N).")

    parser.add_argument("--commit", action="store_true",
                        help="Enable real deletion mode. By default, the script runs in dry-run mode.")

    parser.add_argument("--limit", type=int, default=int(os.getenv("HAND_LIMIT", 1000000)),
                        help="Maximum number of eligible hands to process (default: 1,000,000).")

    parser.add_argument("--type", choices=['cash', 'tourney', 'both'], default=os.getenv("HAND_TYPE", "cash"),
                        help="Select hand type(s) to process: cash, tourney, or both (default: cash).")

    args = parser.parse_args()

    for key, value in db_args.items():
        setattr(args, key, value)

    required_db_fields = ['host', 'dbname', 'user', 'password']
    missing_fields = [f for f in required_db_fields if not getattr(args, f)]

    if missing_fields:
        parser.error(f"Missing required database credentials in .env: {', '.join(f.upper() for f in missing_fields)}")

    if args.days <= 0:
        parser.error("Days must be a positive integer.")
    if args.limit <= 0:
        parser.error("Limit must be a positive integer.")

    if args.type == 'cash':
        args.hand_types = ['cash']
    elif args.type == 'tourney':
        args.hand_types = ['tourney']
    else:
        args.hand_types = ['cash', 'tourney']

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
    try:
        conn = psycopg2.connect(
            host=args.host, dbname=args.dbname, user=args.user, password=args.password
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def execute_bulk_pruning(conn, cursor, hand_type, days, cutoff_date, dbname, dry_run=False, hand_limit=1000000):
    """
    Executes the pruning check sequentially using LIMIT and OFFSET to provide
    granular progress tracking per 100k hand window.
    """
    tables = get_table_names(hand_type)

    print(f"\n[{hand_type.upper()}] Running sequential batch prune operation...")

    BATCH_SIZE = 100000
    total_input_batches = math.ceil(hand_limit / BATCH_SIZE)

    pruned_hand_ids = []

    print(f"Starting granular analysis loop (Total windows to analyze: {total_input_batches:,})")
    print(f"--- Each window processes {BATCH_SIZE:,} hands from oldest to newest ---")

    cumulative_pruned_count = 0
    total_analyzed = 0

    for batch_number in range(total_input_batches):
        offset = batch_number * BATCH_SIZE

        current_limit = min(BATCH_SIZE, hand_limit - offset)
        if current_limit <= 0:
            break

        start_time_batch = time.time()

        hands_to_prune_batch_query = sql.SQL("""
            WITH current_batch_candidates AS (
                SELECT {summary}.id_hand
                FROM {summary}
                WHERE {summary}.date_played <= %s
                ORDER BY {summary}.date_played ASC, {summary}.id_hand ASC
                LIMIT %s
                OFFSET %s
            )
            SELECT cbc.id_hand
            FROM current_batch_candidates cbc
            WHERE NOT EXISTS (
                SELECT 1
                FROM {player_stats}
                JOIN active_players_temp apt ON {player_stats}.id_player = apt.id_player
                WHERE {player_stats}.id_hand = cbc.id_hand
            )
        """).format(
            summary=tables['summary'],
            player_stats=tables['player_stats']
        )

        params = [cutoff_date, current_limit, offset]

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

        total_analyzed = offset + current_limit

        input_window_start = offset + 1
        input_window_end = total_analyzed

        elapsed_batch = time.time() - start_time_batch
        print(
            f"Window {batch_number + 1:,} / {total_input_batches:,} "
            f"(Hands {input_window_start:,}-{input_window_end:,}): "
            f"Pruned this window: {batch_pruned_count:,} | "
            f"Cumulative Pruned: {cumulative_pruned_count:,} "
            f"(Time: {elapsed_batch:.2f}s)"
        )

    hands_to_delete_count = len(pruned_hand_ids)

    print(f"\n[{hand_type.upper()}] Scan complete.")
    print(f"Total hands analyzed by the database: {total_analyzed:,}")
    print(f"Hands found eligible for deletion: {hands_to_delete_count:,}")

    if total_analyzed > 0:
        pruning_ratio_percent = (hands_to_delete_count / total_analyzed) * 100
        print(f"Total pruning ratio: {pruning_ratio_percent:.1f}%")
    else:
        print("Total pruning ratio: N/A (No hands analyzed)")

    print(f"\n[{hand_type.upper()}] Total eligible hands for deletion: {hands_to_delete_count:,} (Dry Run: {dry_run}).")

    if hands_to_delete_count == 0:
        return 0, 0

    if dry_run:
        return hands_to_delete_count, 0

    temp_table_name = sql.Identifier(f'temp_prune_ids_{hand_type}_{int(time.time())}')

    print(f"Loading {hands_to_delete_count:,} hand IDs into temporary table for deletion...")

    cursor.execute(sql.SQL("CREATE TEMP TABLE {} (id_hand BIGINT PRIMARY KEY) ON COMMIT DROP").format(temp_table_name))

    data_to_insert = [(id_hand,) for id_hand in pruned_hand_ids]
    insert_query = sql.SQL("INSERT INTO {} (id_hand) VALUES (%s)").format(temp_table_name)
    cursor.executemany(insert_query, data_to_insert)

    delete_stats_query = sql.SQL("""
        DELETE FROM {player_stats}
        WHERE id_hand IN (SELECT id_hand FROM {temp_table})
    """).format(player_stats=tables['player_stats'], temp_table=temp_table_name)

    print("Executing DELETE on Player Statistics table...")
    cursor.execute(delete_stats_query)
    stats_deleted = cursor.rowcount

    delete_summary_query = sql.SQL("""
        DELETE FROM {summary}
        WHERE id_hand IN (SELECT id_hand FROM {temp_table})
    """).format(summary=tables['summary'], temp_table=temp_table_name)

    print("Executing DELETE on Hand Summary table...")
    cursor.execute(delete_summary_query)
    summary_deleted = cursor.rowcount

    if summary_deleted != hands_to_delete_count:
        print(f"WARNING: Summary rows deleted ({summary_deleted:,}) "
              f"does not match pre-scan count ({hands_to_delete_count:,}).")

    return summary_deleted, stats_deleted


def main():
    args = parse_arguments()

    print("\n--- PT4 Database Pruner ---")
    print(f"Inactivity Threshold (N): {args.days} days")
    print(f"Dry Run Mode: {args.dry_run} (Use --commit flag for real deletions)")
    print(f"Hand Types: {', '.join(args.hand_types).upper()}")
    print(f"Processing Limit: {args.limit:,} hands per type.\n")

    conn = create_connection(args)
    cursor = conn.cursor()

    total_pruned_summary = 0
    start_time = time.time()

    try:
        cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=args.days)).strftime('%Y-%m-%d')
        print(f"Cutoff Date: {cutoff_date} (Hands older than this are eligible for pruning)")

        print("\n--- Initializing Global Active Player Scan ---")
        active_player_cte = sql.SQL("""
            CREATE TEMP TABLE active_players_temp (id_player BIGINT PRIMARY KEY) ON COMMIT DROP;
            INSERT INTO active_players_temp (id_player)
            SELECT DISTINCT id_player
            FROM cash_hand_player_statistics chps_c
            JOIN cash_hand_summary chs_c ON chps_c.id_hand = chs_c.id_hand
            WHERE chs_c.date_played > (CURRENT_DATE - INTERVAL %s)
            UNION
            SELECT DISTINCT id_player
            FROM tourney_hand_player_statistics thps_t
            JOIN tourney_hand_summary ths_t ON thps_t.id_hand = ths_t.id_hand
            WHERE ths_t.date_played > (CURRENT_DATE - INTERVAL %s);
        """)

        cursor.execute(active_player_cte, [f"{args.days} days", f"{args.days} days"])

        cursor.execute("SELECT COUNT(*) FROM active_players_temp")
        active_player_count = cursor.fetchone()[0]
        print(f"[INFO] Created temp table with {active_player_count:,} unique recently active players.")

        for hand_type in args.hand_types:
            summary_table = get_table_names(hand_type)['summary']

            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(summary_table))
            total_hands = cursor.fetchone()[0]
            print(f"\n[{hand_type.upper()}] Total hands in database: {total_hands:,}")

            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE date_played > %s").format(summary_table), (cutoff_date,))
            skipped_hands = cursor.fetchone()[0]
            print(f"[{hand_type.upper()}] Hands newer than cutoff date (retained): {skipped_hands:,}")

            summary_deleted, stats_deleted = execute_bulk_pruning(
                conn, cursor, hand_type, args.days, cutoff_date, args.dbname, args.dry_run, args.limit
            )

            total_pruned_summary += summary_deleted

            print(f"[{hand_type.upper()}] Result:")
            print(f"  Deleted Hands (Summary): {summary_deleted:,}")
            print(f"  Deleted Rows (Statistics): {stats_deleted:,}")

        if not args.dry_run and total_pruned_summary > 0:
            print("\nCommit initiated. Persisting changes to database...")
            conn.commit()
            print(f"**Successfully deleted {total_pruned_summary:,} hands in total.**")

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

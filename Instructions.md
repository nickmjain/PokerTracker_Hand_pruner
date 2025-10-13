PT4 Pruner Execution Guide (PostgreSQL)

This guide outlines how to use pruner.py to surgically remove poker hands where all participating players have been inactive for a defined period. The process runs in controlled batches, adhering strictly to the defined hand limit.

1. Prerequisites and Setup
    1.	Python Environment: Ensure Python 3.x is installed.
    2.	Dependencies: Install the required Python packages. (If using a virtual environment, activate it first.)
            pip install python-dotenv psycopg2-binary
    3.	Database Configuration (.env): The script requires database connection details to be set in a local .env file. These cannot be passed as command-line arguments.
            DB_HOST=your_db_host
            6.	DB_NAME=your_db_name
            DB_USER=your_db_user
            DB_PASSWORD=your_db_password

2. Core Execution Modes
The script is designed for safety and defaults to a Dry Run.

    A. Dry Run (Analysis Only)
    This mode analyzes the database, identifies all eligible hands, calculates the pruning ratio, and prints a summary. No data is deleted. This is the default mode.
    
    Example: Analyze the oldest 5,000,000 cash hands older than 365 days.
        python pruner.py --type cash --limit 5000000 --days 365

    B. Live Commit (Deletion)
    To proceed with the actual deletion of the identified hands, you MUST include the --commit flag.
    
    Example: Delete the 5,000,000 analyzed cash hands.
        python pruner.py --type cash --limit 5000000 --commit

3. Key Arguments
    Argument	Function	Default Value	Notes
    --type	Specifies the type of hands to process.	cash	Options: cash, tourney, or both.
    --limit	The maximum total number of oldest hands to analyze across all batches.	1,000,000	Controls the scope of the operation.
    --days	Inactivity threshold. Hands are eligible if every player in the hand has been inactive for this many days.	365	
    --commit	Activates real database deletion.	(Dry Run)	Required for permanent changes.

4. Critical Post-Action
After a successful deletion (--commit), you MUST manually run the following PostgreSQL commands to physically reclaim the space and ensure database efficiency. This is non-negotiable for performance optimization.

VACUUM FULL;
REINDEX DATABASE {your_db_name};

These are the instructions for efficient, surgical pruning of your database. Let me know if you'd like a brief explanation of the SQL CTE (Common Table Expression) logic being used, as it's the core of the script's performance.
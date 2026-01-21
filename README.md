JCL Analysis & Extraction Engine
A specialized Python-based extraction engine designed to scan, resolve, and transform legacy IBM z/OS Job Control Language (JCL) into structured, relational data within a PostgreSQL environment.
Overview
This tool automates the "intelligence gathering" phase of mainframe modernization. By utilizing a Lark LALR parser, it resolves the complex internal logic of JCL—including nested procedures (PROCs), include members, and symbolic variable substitution—to output a queryable map of program executions and file allocations.
Key Features
* Advanced Preprocessing: Truncates sequence areas (cols 73-80).
* Logical Resolution: Handles multi-line JCL continuations and trailing comment stripping.
* Recursive Expansion: Recursively expands INCLUDE members and PROC blocks.
* Symbolic Resolution: Fully resolves &VARIABLE references using a hierarchical search order (SET statements -> PROC defaults -> EXEC overrides).
* Lark LALR Parsing: Employs a robust grammar with strict token priorities to accurately distinguish between labels, opcodes, and complex parameter values.
* PostgreSQL Integration:
   * Utilizes native JSONB for flexible attribute storage.
   * Features a flattened schema for high-performance indexing of LRECL, BLKSIZE, and RECFM.
   * Supports ON CONFLICT logic and RETURNING clauses for relational integrity.
* Environment Agnostic: Operates on both z/OS PDS structures and standard Linux/Windows/Mac file systems via the SYSTEM configuration.
Tech Stack
* Language: Python 3.x
* Parser Framework: Lark (LALR)
* Database: PostgreSQL
* Database Driver: psycopg2
Installation
1. Clone the repository:
git clone [https://github.com/ghjoss/jcl-extraction-engine.git](https://github.com/ghjoss/jcl-extraction-engine.git)
cd jcl-extraction-engine

2. Install dependencies:
This assumes Python is installed.
pip install lark psycopg2

3. Database Setup:
Ensure you have a PostgreSQL instance running and create a target database:
CREATE DATABASE jcl_db;

Configuration
The tool is controlled via a config.json file. Create this in the root directory:
Parameter
	Type
	Description
	SYSTEM
	String
	Z for z/OS or LWM for Linux/Windows/Mac.
	FILE
	String
	The primary JCL member name to process.
	PATH
	String
	The base directory or PDS containing the source.
	LIB
	Array
	(Optional) List of search paths for INCLUDES/PROCs.
	DATABASE
	String
	PostgreSQL database name. Must be created prior to execution.
	USER
	String
	Database login ID. Default: postgres.
	PASSWORD
	String
	Password for the database user.
	DROP_TABLES
	Boolean
	If true, resets the database schema on startup.
	Usage
   1. Configure your environment in config.json.
   2. Run the engine:
python jcl_parser.py

Database Schema
The engine populates three primary tables:
      1. PROJECTS: High-level grouping for different JCL sets.
      2. STEPS: Each execution step (EXEC), including program names and resolved parameters.
      3. DATA_ALLOCATIONS: Every file allocation (DD), including DSNs, Dispositions, and DCB attributes.
Note: This tool is designed for analysis and inventory purposes as part of legacy modernization initiatives.
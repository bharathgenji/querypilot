# QueryPilot: A Schema-Aware SQL Query Generator

## Overview

QueryPilot is a Django-based application designed to handle natural language queries, generate SQL, validate queries against database schemas, and execute them seamlessly. This project integrates Pinecone for semantic search, Defog SQLCoder for SQL generation, and PostgreSQL for database management.

## Features

### Natural Language to SQL Conversion:
Translates natural language queries into SQL using fine-tuned language models.

### Schema-Aware Query Generation:
Ensures generated SQL queries conform to the database schema.

### Validation and Correction:
Validates SQL queries for syntax, schema compliance, and security.
Automatically corrects hallucinated or invalid columns and table names.

### Integration with Pinecone:
Uses Pinecone to store and retrieve schema embeddings for better query understanding.

### Scalable Design:
Modular components for seamless extensibility and integration with other systems.

## Technologies Used
Django: Backend framework for API development.
PostgreSQL: Database for schema and data storage.
Pinecone: Vector database for schema embeddings.
Defog SQLCoder: Pre-trained language model for SQL generation.


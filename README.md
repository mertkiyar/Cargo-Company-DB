# Cargo Company Database Project

This project contains a database system for a cargo company, including tables, data generation with AI, and analysis queries.

## File Guide
*   **create-tables.sql**: Creates the database structure (Tables, Enums).
*   **insert-data.sql**: Generates sample data (Customers, Shipments, etc.).
*   **queries.sql**: SQL queries to analyze the data.
*   **trigger-*.sql**: Automated rules for package returns and logging.
*   **EER-Diagram.pdf**: EER Diagram of the project.
*   **Generated-UML-Diagram.png**: UML Diagram that generated via pgAdmin 4. 

## General Tables
*   **Shipment / Package**: Core tables for tracking items.
*   **Customer**: Stores Individual and Corporate customers.
*   **Delivery / Vehicle**: Manages couriers, vehicles, and delivery status.
*   **Invoice / Payment**: Financial records.
*   **Employee**: Branch staff and couriers.

## Data Counts
The `insert-data.sql` script creates approximately:
*   **3,000** Shipments (containing ~5,000 packages)
*   **1,000** Customers
*   **400+** Employees (across 50 Branches)
*   **100** Vehicles

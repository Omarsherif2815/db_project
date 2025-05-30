This repository contains the implementation of a custom lightweight Database Management System (DBMS), developed as part of the CSEN604 - Databases II course at the German University in Cairo, Spring 2025.

## 📚 Overview

This project is divided into two milestones:

### ✅ Milestone 1 – Core DBMS Features

Implemented basic DBMS features such as:
- Creating tables with string-type columns
- Inserting records into tables
- Selecting records:
  - Select all records
  - Select by value conditions (WHERE-like)
  - Select using direct page/record pointer
- Retrieving trace logs of table operations

### 🧠 Milestone 2 – Advanced Indexing & Recovery

Extended the DBMS to support:
- **Bitmap indexing** on specific table columns
- **Indexed-based record selection**
- **Automatic index updates** on data insertion
- **Recovery** of deleted pages using trace validation
## 🛠️ Features

### Table Management
- `createTable(String tableName, String[] columnNames)`
- Records stored as `String[]` arrays, with no primary key or null values

### Record Operations
- `insert(String tableName, String[] record)`
- `select(...)` for different selection conditions
- `getFullTrace(String tableName)` and `getLastTrace(String tableName)`

### Bitmap Indexing
- `createBitMapIndex(String tableName, String colName)`
- `getValueBits(String tableName, String colName, String value)`
- `selectIndex(String tableName, String[] cols, String[] vals)`

### Data Recovery
- `validateRecords(String tableName)`
- `recoverRecords(String tableName, ArrayList<String[]> missing)`

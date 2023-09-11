# Sequel

Software to do multiple things with your database

## Warning
This project is for my learning purposes and is not meant to be used anywhere.

## Functions

Has 4 programs bundled into a cli app:

1. Database Schema Analyzer: Compare database schemas from multiple sources and generate Excel reports

2. Stored Procedure Analyzer: Compare Stored Procedures from multiple databases on your system and generate Excel report
   - Online SP Comparator among multiple databases. Compares contents as well. Comparison done against a source.
   - Online SP Presence Analyser among multiple databases. Makes a superset of all SPs and checks which SP is absent in what database.
   - Offline SP Presence Analyzer for multiple databases. Superset is made. No content comparison is done.
   - Offline SP Comparator among multiple databases. Comparison done against a source.
  
3. Stored Procedure Comparator + HTML Diff Generator: Compare Stored Procedures between two databases on your system, generate Excel reports, and store differential files in HTML format for visualizing differences
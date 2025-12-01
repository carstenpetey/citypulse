# 🗽 CityPulse

Citypulse is a project utilizing open-source data to analyze inequality among different neighborhoods and boroughs in New York City. Cleaned CSV files is in the "datasets" directory; code used to clean the open-source data is in the "cleaning datasets" directory; the visualizations made with these datasets are in the "visualizations" directory.

# Project Overview

Goal:
To compare how quality-of-life indicators differ across different boroughs and neighborhoods. CityPulse can help residents, policymakers, and public service agencies answer key questions about how their borough is evolving, such as:
- Is my neighborhood becoming safer or more stressed over time?
- Where is housing pressure (rent burden and evictions) increasing the most?
- How does transit experience differ across boroughs?


Tech Stack:
Component	| Technology
Data Sources	| NYC Open Data
Cleaning & Prep	| Python, Pandas, Jupyter Notebook
Visualization	| Tableau Public
Storage	| Google Drive


Dataset Summary
Dataset	| Description
NYC Shooting Incident Data (2025)	| Daily shootings with borough, time, location type
Rent Burden | (%) by Neighborhood	Share of households spending >30% income on rent
Eviction Change 2023–2025	| Year-over-year changes in eviction filings
Subway Satisfaction Survey	| Rider perceptions of affordability & reliability


How CityPulse Works
1. Data Collection
- All raw datasets pulled from NYC Open Data.

2. Cleaning (Python)
- Handle nulls & duplicates
- Filter to relevant years
- Export clean CSVs

3. Visualization (Tableau)
- Line charts, bar charts, scatterplot, maps
- Dashboard

4. Dashboard Output
A clean interface showing how each borough differs in crime, housing, and transit patterns.


📎 Authors

Team CityPulse — DS3 / UCSD
@carstenpetey @nghi-nguyen23 @cindysgit

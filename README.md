Vinyl Critics vs Streams

This project examines whether Pitchfork review scores align with Spotify and YouTube streaming performance. The focus is on building a clean workflow from raw data to reproducible analysis, and identifying cases where critical reception diverges from mainstream listening habits.

Project Objectives

• Build a small but realistic ETL process from raw Pitchfork and Spotify/YouTube datasets.
• Standardize and clean review data, including multi-artist reviews and date parsing.
• Create a warehouse in SQLite with clearly defined dimension and fact tables.
• Perform entity resolution between Pitchfork artist names and Spotify artist names.
• Produce SQL views that act as a stable analysis interface.
• Explore how critic sentiment compares with real streaming behavior.
• Highlight atypical cases such as high-scoring artists with lower streaming counts or vice versa.

Data Pipeline Overview

The project repository is organised into the following components:

Raw Data
Stored under data/raw/.
• Pitchfork SQLite dump (reviews, artists, genres, labels)
• Spotify and YouTube track metrics

Staging and Cleaning
Scripts located in the scripts/ directory.
• Parsing and typing of Pitchfork review fields
• Splitting multi-artist reviews into a bridge table
• Applying fuzzy string matching with rapidfuzz for cross-dataset artist mapping

Data Warehouse
The warehouse is stored as data/processed/vinyl_dw.sqlite.
SQL files defining the semantic layer are under sql/dw/.
Important views include:
• vw_review_with_artist
• vw_unmatched_artists
• vw_artist_summary
• vw_artist_streams
• vw_artist_critics_vs_streams

These views are the main entry points for analysis, keeping notebooks and dashboards independent of raw schema details.

Analytical Notebook

The main exploratory work is in notebooks/01_critics_vs_streams.ipynb.
The notebook performs:
• Loading of the warehouse using SQLite and pandas
• Checks on schema, missing values, and data ranges
• Log-scaling of streaming metrics to reduce skew
• Correlation checks between Pitchfork scores and stream counts
• Identification of outliers (e.g., “underrated” or “overrated” artists)
• Visualisation of the critic-score vs streaming relationship

Key Visualisation

The scatter plot below compares average Pitchfork score with the log of total Spotify streams. Artists with at least two reviews and at least five available tracks are included. A small number of outliers are labelled for clarity.

![Critics vs Streams](assets/critic_vs_streams_labeled.png)

Interpretation:
• The overall correlation between critic favourability and streaming scale is weak.
• Artists such as Post Malone, Coldplay, Sia, Red Hot Chili Peppers, and Green Day have high streaming counts but only mid-level critical scores.
• Highly acclaimed artists such as John Coltrane, Ennio Morricone, and Caetano Veloso remain below the top streaming tier.
• The result reflects a common industry pattern: critics tend to reward originality and artistic influence, while streaming behaviour is influenced by virality, playlists, marketing, and algorithmic exposure.

| Artist                | Avg Score | Log10 Streams | Reviews | Tracks |
| --------------------- | --------- | ------------- | ------- | ------ |
| Post Malone           | 4.85      | 10.18         | 2       | 10     |
| Coldplay              | 5.72      | 10.07         | 10      | 10     |
| Sia                   | 5.85      | 9.87          | 4       | 10     |
| Red Hot Chili Peppers | 5.60      | 9.83          | 5       | 10     |
| Green Day             | 5.72      | 9.66          | 5       | 10     |
| John Coltrane         | 9.50      | 8.61          | 3       | 10     |
| Ennio Morricone       | 8.57      | 8.42          | 3       | 10     |
| Caetano Veloso        | 8.84      | 8.31          | 5       | 10     |

Technical Stack

• Python (pandas, numpy, matplotlib)
• SQLite for the warehouse
• rapidfuzz for fuzzy matching
• Jupyter Notebook for exploration
• Modular ETL scripts for loading and transformations

Possible Extensions

• Improve artist matching with manual overrides or additional metadata
• Incorporate Spotify popularity metrics and playlist counts
• Expand analysis to track-level comparisons
• Add a small dashboard built on the final semantic view

Purpose of the Project

This repository demonstrates an end-to-end workflow covering data cleaning, fuzzy matching, warehouse modelling, SQL view creation, exploratory analysis, and communicating findings. It is meant to act as a compact but realistic example of how a small analytics/engineering project is structured from start to finish.
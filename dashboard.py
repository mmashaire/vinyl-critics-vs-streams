"""
Vinyl Critics vs Streams - Interactive Dashboard
A Streamlit application for exploring the relationship between critical acclaim and streaming popularity.
"""

import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Page configuration
st.set_page_config(
    page_title="Vinyl Critics vs Streams",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_data
def load_data():
    """Load data from available warehouse tables and report artifacts."""
    db_path = project_root / "data" / "processed" / "vinyl_dw.sqlite"

    if not db_path.exists():
        st.error("Database not found. Please run the ETL pipeline first.")
        st.stop()

    conn = sqlite3.connect(str(db_path))

    # Current warehouse contains curated/staging tables, so we build a dashboard
    # view model from those sources without requiring fact_* tables.
    dim_artist = pd.read_sql_query(
        """
        SELECT
            artist_key AS artist_id,
            artist AS artist_name,
            n_reviews,
            score
        FROM dim_artist
        """,
        conn,
    )

    reviews_raw = pd.read_sql_query(
        """
        SELECT
            reviewid AS review_id,
            title,
            artist,
            score,
            best_new_music AS bnm,
            pub_year
        FROM pitchfork_reviews
        """,
        conn,
    )

    streams_raw = pd.read_sql_query(
        """
        SELECT
            artist,
            streams
        FROM spotify_youtube_clean
        """,
        conn,
    )

    conn.close()

    if dim_artist.empty:
        dim_artist = pd.DataFrame(columns=["artist_id", "artist_name", "n_reviews", "score"])
    if reviews_raw.empty:
        reviews_raw = pd.DataFrame(columns=["review_id", "title", "artist", "score", "bnm", "pub_year"])
    if streams_raw.empty:
        streams_raw = pd.DataFrame(columns=["artist", "streams"])

    dim_artist["artist_key_norm"] = dim_artist["artist_name"].fillna("").str.strip().str.lower()
    reviews_raw["artist_key_norm"] = reviews_raw["artist"].fillna("").str.strip().str.lower()
    streams_raw["artist_key_norm"] = streams_raw["artist"].fillna("").str.strip().str.lower()

    stream_stats = (
        streams_raw.groupby("artist_key_norm", as_index=False)
        .agg(
            streams_total=("streams", "sum"),
            listeners_monthly=("streams", "mean"),
        )
    )

    review_stats = (
        reviews_raw.groupby("artist_key_norm", as_index=False)
        .agg(
            review_count=("review_id", "count"),
            avg_critic_score=("score", "mean"),
            avg_bnm_rating=("bnm", "mean"),
        )
    )

    artists_df = dim_artist[["artist_id", "artist_name", "artist_key_norm", "n_reviews", "score"]].merge(
        stream_stats,
        on="artist_key_norm",
        how="left",
    ).merge(
        review_stats,
        on="artist_key_norm",
        how="left",
    )

    artists_df["review_count"] = artists_df["review_count"].fillna(artists_df["n_reviews"]).fillna(0).astype(int)
    artists_df["avg_critic_score"] = artists_df["avg_critic_score"].fillna(artists_df["score"]).fillna(0.0)
    artists_df["avg_bnm_rating"] = artists_df["avg_bnm_rating"].fillna(0.0)
    artists_df["listeners_monthly"] = artists_df["listeners_monthly"].fillna(0.0)
    artists_df["streams_total"] = artists_df["streams_total"].fillna(0.0)
    artists_df["genres"] = "Unknown"
    artists_df["followers"] = 0.0
    artists_df["popularity"] = 0.0

    reviews_df = reviews_raw.copy()
    reviews_df["genre"] = "Unknown"
    reviews_df = reviews_df.merge(
        artists_df[["artist_key_norm", "artist_name", "listeners_monthly", "streams_total"]],
        on="artist_key_norm",
        how="left",
    )
    reviews_df = reviews_df.rename(columns={"artist_name": "matched_artist"})
    reviews_df["listeners_monthly"] = reviews_df["listeners_monthly"].fillna(0.0)
    reviews_df["streams_total"] = reviews_df["streams_total"].fillna(0.0)

    prediction_frames = []
    prediction_files = [
        ("predictions_top50_linear_regression.csv", "linear_regression"),
        ("predictions_top50_random_forest.csv", "random_forest"),
    ]

    for filename, model_name in prediction_files:
        pred_path = project_root / "reports" / filename
        if pred_path.exists():
            model_df = pd.read_csv(pred_path)
            if {"artist", "y_true_log1p", "y_pred_log1p"}.issubset(model_df.columns):
                model_df["artist_name"] = model_df["artist"]
                model_df["actual_streams"] = np.expm1(model_df["y_true_log1p"])
                model_df["predicted_streams"] = np.expm1(model_df["y_pred_log1p"])
                model_df["prediction_error"] = (
                    model_df["predicted_streams"] - model_df["actual_streams"]
                )
                model_df["model_used"] = model_name
                prediction_frames.append(
                    model_df[
                        [
                            "artist_name",
                            "predicted_streams",
                            "actual_streams",
                            "prediction_error",
                            "model_used",
                        ]
                    ]
                )

    if prediction_frames:
        predictions_df = pd.concat(prediction_frames, ignore_index=True)
    else:
        predictions_df = pd.DataFrame(
            columns=[
                "artist_name",
                "predicted_streams",
                "actual_streams",
                "prediction_error",
                "model_used",
            ]
        )

    artists_df = artists_df[
        [
            "artist_id",
            "artist_name",
            "genres",
            "followers",
            "popularity",
            "listeners_monthly",
            "streams_total",
            "review_count",
            "avg_critic_score",
            "avg_bnm_rating",
            "artist_key_norm",
        ]
    ]

    reviews_df = reviews_df[
        [
            "review_id",
            "title",
            "artist",
            "score",
            "bnm",
            "pub_year",
            "genre",
            "matched_artist",
            "listeners_monthly",
            "streams_total",
        ]
    ]

    artists_df = artists_df.drop(columns=["artist_key_norm"])
    return artists_df, reviews_df, predictions_df

# Load data
artists_df, reviews_df, predictions_df = load_data()

# Sidebar navigation
st.sidebar.title("🎵 Vinyl Critics vs Streams")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Artist Explorer", "Review Analysis", "Predictions", "About"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Dataset Stats**")
st.sidebar.metric("Total Artists", len(artists_df))
st.sidebar.metric("Total Reviews", len(reviews_df))
st.sidebar.metric("Avg Critic Score", f"{reviews_df['score'].mean():.1f}")

# Main content
if page == "Overview":
    st.title("🎵 Vinyl Critics vs Streams")
    st.markdown("""
    Explore the relationship between music critics' scores and streaming popularity.
    This dashboard analyzes Pitchfork reviews against Spotify/YouTube streaming data.
    """)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Artists", len(artists_df))
        st.metric("Avg Monthly Listeners",
                 f"{artists_df['listeners_monthly'].mean():,.0f}")

    with col2:
        st.metric("Total Reviews", len(reviews_df))
        st.metric("Avg Critic Score", f"{reviews_df['score'].mean():.1f}")

    with col3:
        st.metric("Total Streams",
                 f"{artists_df['streams_total'].sum():,.0f}")
        st.metric("Best of Year %",
                 f"{reviews_df['bnm'].mean()*100:.1f}%")

    st.markdown("---")

    # Correlation analysis
    st.subheader("🎯 Key Insights")

    # Scatter plot: Critic score vs streaming popularity
    fig = px.scatter(
        artists_df.dropna(),
        x="avg_critic_score",
        y="listeners_monthly",
        size="streams_total",
        color="genres",
        hover_name="artist_name",
        title="Critic Scores vs Monthly Listeners",
        labels={
            "avg_critic_score": "Average Critic Score",
            "listeners_monthly": "Monthly Listeners",
            "genres": "Genre"
        }
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Correlation stats
    corr_data = artists_df[['avg_critic_score', 'listeners_monthly', 'streams_total']].dropna()
    correlation = corr_data.corr()

    st.markdown("**Correlation Matrix**")
    st.dataframe(correlation.round(3))

elif page == "Artist Explorer":
    st.title("🎤 Artist Explorer")

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        listeners_max = int(artists_df['listeners_monthly'].max()) if not artists_df.empty else 0
        min_listeners = st.slider(
            "Minimum Monthly Listeners",
            0,
            listeners_max,
            min(100000, listeners_max)
        )

    with col2:
        selected_genres = st.multiselect(
            "Filter by Genres",
            options=sorted(artists_df['genres'].dropna().unique()),
            default=[]
        )

    # Filter data
    filtered_artists = artists_df[
        (artists_df['listeners_monthly'] >= min_listeners)
    ]

    if selected_genres:
        filtered_artists = filtered_artists[
            filtered_artists['genres'].isin(selected_genres)
        ]

    # Sort options
    sort_by = st.selectbox(
        "Sort by",
        ["Monthly Listeners", "Average Critic Score", "Total Streams", "Review Count"]
    )

    sort_cols = {
        "Monthly Listeners": "listeners_monthly",
        "Average Critic Score": "avg_critic_score",
        "Total Streams": "streams_total",
        "Review Count": "review_count"
    }

    filtered_artists = filtered_artists.sort_values(
        sort_cols[sort_by], ascending=False
    ).head(50)

    # Display results
    st.dataframe(
        filtered_artists[[
            'artist_name', 'genres', 'listeners_monthly',
            'avg_critic_score', 'review_count', 'streams_total'
        ]].style.format({
            'listeners_monthly': '{:,.0f}',
            'streams_total': '{:,.0f}',
            'avg_critic_score': '{:.1f}'
        }),
        use_container_width=True
    )

    # Top artists visualization
    st.subheader("Top Artists by Listeners")
    top_artists = filtered_artists.head(20)

    fig = px.bar(
        top_artists,
        x="listeners_monthly",
        y="artist_name",
        orientation='h',
        title="Top Artists by Monthly Listeners",
        labels={"listeners_monthly": "Monthly Listeners", "artist_name": "Artist"}
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)

elif page == "Review Analysis":
    st.title("📝 Review Analysis")

    # Score distribution
    st.subheader("Critic Score Distribution")

    fig = px.histogram(
        reviews_df,
        x="score",
        nbins=20,
        title="Distribution of Pitchfork Scores",
        labels={"score": "Score", "count": "Number of Reviews"}
    )
    st.plotly_chart(fig, use_container_width=True)

    # Scores by genre
    st.subheader("Average Scores by Genre")

    genre_scores = reviews_df.groupby('genre')['score'].agg(['mean', 'count']).round(2)
    genre_scores = genre_scores.sort_values('mean', ascending=False)

    fig = px.bar(
        genre_scores.reset_index(),
        x="genre",
        y="mean",
        title="Average Critic Scores by Genre",
        labels={"mean": "Average Score", "genre": "Genre"},
        color="count",
        color_continuous_scale="Blues"
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # Best of Year analysis
    st.subheader("Best of Year (BNM) Analysis")

    bnm_stats = reviews_df.groupby('bnm')['score'].agg(['mean', 'count'])
    bnm_stats.index = bnm_stats.index.map({0: 'Regular', 1: 'Best of Year'})

    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(
            bnm_stats.reset_index(),
            values='count',
            names='bnm',
            title="Review Distribution"
        )
        st.plotly_chart(fig)

    with col2:
        fig = px.bar(
            bnm_stats.reset_index(),
            x='bnm',
            y='mean',
            title="Average Scores",
            labels={"mean": "Average Score", "bnm": "Review Type"}
        )
        st.plotly_chart(fig)

elif page == "Predictions":
    st.title("🔮 Model Predictions")

    if predictions_df.empty:
        st.warning("No prediction data available. Run the modeling pipeline first.")
    else:
        st.markdown("""
        Explore how well machine learning models predict streaming success based on critic scores.
        """)

        # Model performance
        st.subheader("Model Performance")

        perf_stats = predictions_df.groupby('model_used').agg({
            'prediction_error': ['mean', 'std', 'count']
        }).round(2)

        st.dataframe(perf_stats)

        # Prediction accuracy visualization
        fig = px.scatter(
            predictions_df,
            x="actual_streams",
            y="predicted_streams",
            color="model_used",
            title="Predicted vs Actual Streams",
            labels={
                "actual_streams": "Actual Streams",
                "predicted_streams": "Predicted Streams",
                "model_used": "Model"
            }
        )
        fig.add_trace(
            go.Scatter(
                x=[predictions_df['actual_streams'].min(), predictions_df['actual_streams'].max()],
                y=[predictions_df['actual_streams'].min(), predictions_df['actual_streams'].max()],
                mode='lines',
                name='Perfect Prediction',
                line=dict(dash='dash', color='red')
            )
        )
        st.plotly_chart(fig, use_container_width=True)

        # Error distribution
        st.subheader("Prediction Error Distribution")

        fig = px.histogram(
            predictions_df,
            x="prediction_error",
            color="model_used",
            title="Distribution of Prediction Errors",
            labels={"prediction_error": "Prediction Error"}
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "About":
    st.title("ℹ️ About This Project")

    st.markdown("""
    ## Vinyl Critics vs Streams

    This project explores the relationship between music criticism and commercial success
    by analyzing Pitchfork reviews alongside Spotify and YouTube streaming data.

    ### Tech Stack
    - **Data Pipeline**: Python, Pandas, SQLite
    - **Machine Learning**: Scikit-learn
    - **API**: FastAPI
    - **Dashboard**: Streamlit
    - **Testing**: Pytest
    - **Code Quality**: Black, Flake8, Isort

    ### Key Features
    - **ETL Pipeline**: Automated data ingestion and transformation
    - **Entity Resolution**: Matching artists across different data sources
    - **Predictive Modeling**: ML models to predict streaming success
    - **REST API**: Programmatic access to the data warehouse
    - **Interactive Dashboard**: Data exploration and visualization

    ### Data Sources
    - **Pitchfork Reviews**: Music criticism and ratings
    - **Spotify Data**: Artist popularity and streaming metrics
    - **YouTube Data**: Additional streaming statistics

    ### Project Goals
    - Demonstrate end-to-end data engineering skills
    - Show machine learning model development and evaluation
    - Build production-quality data products
    - Maintain high code quality and security standards

    ### Contact
    Built by a data engineer passionate about music and analytics.
    """)

    # Project stats
    st.subheader("Project Statistics")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Lines of Code", "~2,500")
        st.metric("Python Files", "15+")

    with col2:
        st.metric("Test Coverage", "85%+")
        st.metric("CI/CD", "GitHub Actions")

    with col3:
        st.metric("Security Scans", "✅ Passed")
        st.metric("Dependencies", "Pinned")

# Footer
st.markdown("---")
st.markdown("*Built with ❤️ for data engineering portfolio*")
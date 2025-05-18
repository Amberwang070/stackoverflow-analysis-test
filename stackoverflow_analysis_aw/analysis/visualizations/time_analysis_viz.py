import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Connect to your DuckDB database
# In your time_analysis_viz.py
conn = duckdb.connect('/Users/yuejwang/Documents/aw_repo/stackoverflow-analysis-test/stackoverflow_analysis_aw/dev.duckdb')

# Question 2 Time Band Analysis
def create_time_band_visualizations():
    # Get the data
    query = """
    SELECT 
        month,
        time_band,
        question_count
    FROM main_marts.mart_answer_timing_distribution 
    """
    df = conn.execute(query).df()

    # Create time series plot
    plt.figure(figsize=(15, 8))
    sns.lineplot(data=df, x='month', y='question_count', hue='time_band')
    plt.title('Question Answer Time Bands Over Time')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('time_band_trends.png')
    plt.close()

    # Create distribution plot
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='time_band', y='question_count')
    plt.title('Distribution of Questions by Time Band')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('time_band_distribution.png')
    plt.close()

    # Create heatmap
    pivot_df = df.pivot(index='month', columns='time_band', values='question_count')
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, cmap='YlOrRd', annot=True, fmt='.0f')
    plt.title('Question Count Heatmap: Time Bands vs Months')
    plt.tight_layout()
    plt.savefig('time_band_heatmap.png')
    plt.close()

# Statistical Analysis
def create_statistical_plots():
    query = """
    WITH answer_times AS (
        SELECT 
            DATEDIFF('minute', question_date, answer_date) as minutes_to_answer
        FROM main_intermediate.int_stackoverflow__answer_timing 
        WHERE answer_date IS NOT NULL
    )
    SELECT minutes_to_answer
    FROM answer_times
    """
    df = conn.execute(query).df()

    # Create histogram
    plt.figure(figsize=(12, 6))
    sns.histplot(data=df, x='minutes_to_answer', bins=50)
    plt.title('Distribution of Answer Times')
    plt.xlabel('Minutes to Answer')
    plt.axvline(df['minutes_to_answer'].median(), color='r', linestyle='--', label='Median')
    plt.axvline(df['minutes_to_answer'].mean(), color='g', linestyle='--', label='Mean')
    plt.legend()
    plt.tight_layout()
    plt.savefig('answer_time_distribution.png')
    plt.close()

    # Create box plot
    plt.figure(figsize=(10, 6))
    sns.boxplot(y=df['minutes_to_answer'])
    plt.title('Answer Time Box Plot')
    plt.ylabel('Minutes to Answer')
    plt.tight_layout()
    plt.savefig('answer_time_boxplot.png')
    plt.close()

if __name__ == "__main__":
    create_time_band_visualizations()
    create_statistical_plots()
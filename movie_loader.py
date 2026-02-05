import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import ast

def load_movies_dataset(filepath: str = 'data_movies_clean.csv') -> pd.DataFrame:
    # Use latin-1 encoding for broader character support and skip bad lines
    # Use utf-8-sig to handle potential BOM and latin-1 fallback
    try:
        df = pd.read_csv(filepath, low_memory=False, on_bad_lines='skip', encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, low_memory=False, on_bad_lines='skip', encoding='latin-1')
    
    # Clean column names (handle trailing delimiters like ;;; and potential BOM/spaces)
    df.columns = [col.strip().split(';')[0] if isinstance(col, str) else col for col in df.columns]
    
    # Ensure 'id' exists even if it was renamed by BOM
    if 'ï»¿id' in df.columns:
        df = df.rename(columns={'ï»¿id': 'id'})
    
    # Coerce numeric columns to handle mixed types in full dataset
    numeric_cols = ['budget', 'revenue', 'runtime', 'vote_average']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def clean_movie_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    df['adult'] = df['adult'].map({'TRUE': True, 'FALSE': False, True: True, False: False})
    
    numeric_columns = ['budget', 'revenue', 'runtime', 'popularity', 'vote_average', 'vote_count']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['release_year'] = df['release_date'].dt.year
    
    list_columns = ['genre_names', 'production_company_names', 'production_country_names', 
                    'spoken_language_names', 'origin_country']
    for col in list_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_list_field)
    
    return df

def parse_list_field(value) -> List[str]:
    if pd.isna(value):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return parsed
            return [str(parsed)]
        except:
            return []
    return []

def filter_movies(df: pd.DataFrame, 
                  min_year: Optional[int] = None,
                  max_year: Optional[int] = None,
                  min_popularity: Optional[float] = None,
                  min_vote_average: Optional[float] = None,
                  min_vote_count: Optional[int] = None,
                  languages: Optional[List[str]] = None,
                  countries: Optional[List[str]] = None,
                  adult: Optional[bool] = False) -> pd.DataFrame:
    
    filtered = df.copy()
    
    if min_year is not None and 'release_year' in filtered.columns:
        filtered = filtered[filtered['release_year'] >= min_year]
    
    if max_year is not None and 'release_year' in filtered.columns:
        filtered = filtered[filtered['release_year'] <= max_year]
    
    if min_popularity is not None:
        filtered = filtered[filtered['popularity'] >= min_popularity]
    
    if min_vote_average is not None:
        filtered = filtered[filtered['vote_average'] >= min_vote_average]
    
    if min_vote_count is not None:
        filtered = filtered[filtered['vote_count'] >= min_vote_count]
    
    if languages is not None:
        filtered = filtered[filtered['original_language'].isin(languages)]
    
    if countries is not None:
        filtered = filtered[filtered['origin_country'].apply(
            lambda x: any(c in x for c in countries) if isinstance(x, list) else False
        )]
    
    if adult is not None:
        filtered = filtered[filtered['adult'] == adult]
    
    return filtered

def preprocess_for_dht(df: pd.DataFrame) -> List[Dict]:
    movies = []
    
    for _, row in df.iterrows():
        movie = {
            'id': int(row['id']) if pd.notna(row.get('id')) else None,
            'title': str(row['title']) if pd.notna(row.get('title')) else 'Unknown',
            'year': int(row['release_year']) if pd.notna(row.get('release_year')) else None,
            'genres': row.get('genre_names', []),
            'popularity': float(row['popularity']) if pd.notna(row.get('popularity')) else 0.0,
            'rating': float(row['vote_average']) if pd.notna(row.get('vote_average')) else 0.0,
            'vote_count': int(row['vote_count']) if pd.notna(row.get('vote_count')) else 0,
            'runtime': int(row['runtime']) if pd.notna(row.get('runtime')) else 0,
            'budget': int(row['budget']) if pd.notna(row.get('budget')) else 0,
            'revenue': int(row['revenue']) if pd.notna(row.get('revenue')) else 0,
            'language': str(row['original_language']) if pd.notna(row.get('original_language')) else 'unknown',
            'countries': row.get('origin_country', []),
        }
        movies.append(movie)
    
    return movies

def get_movie_sample(filepath: str = 'data_movies_clean.csv', 
                     n_samples: int = 1000,
                     min_year: int = 2000,
                     max_year: int = 2020,
                     min_popularity: float = 10.0,
                     min_vote_count: int = 100) -> Tuple[pd.DataFrame, List[Dict]]:
    
    df = load_movies_dataset(filepath)
    df = clean_movie_data(df)
    df = filter_movies(df, 
                       min_year=min_year, 
                       max_year=max_year,
                       min_popularity=min_popularity,
                       min_vote_count=min_vote_count,
                       languages=['en'],
                       adult=False)
    
    df = df.dropna(subset=['title', 'release_year'])
    
    if len(df) > n_samples:
        df = df.sample(n=n_samples, random_state=42)
    
    movies = preprocess_for_dht(df)
    
    return df, movies

if __name__ == '__main__':
    print("Loading and preprocessing movie dataset...")
    df, movies = get_movie_sample(n_samples=100)
    print(f"Loaded {len(movies)} movies")
    print(f"\nSample movie:")
    print(movies[0])
    print(f"\nDataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

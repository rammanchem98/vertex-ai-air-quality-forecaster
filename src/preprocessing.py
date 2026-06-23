import pandas as pd

def get_preprocessing(df: pd.DataFrame, is_inference: bool = False) -> pd.DataFrame:
    # 1. Common renaming across both pipelines
    df = df.rename(columns={
        'city_name': 'city',
        'rain_1h': 'precipitation',
        'wind_deg': 'wind_direction',
        'collection_time_utc': 'update_timestamp'
    })

    # Base features required by the model matrix (Exactly 10 features)
    final_cols = [
        "temp", "feels_like", "temp_max", "temp_min", "pressure",
        "humidity", "sea_level", "precipitation", "wind_speed", "wind_direction"
    ]

    if not is_inference:
        # 2. Training-specific target generation
        df = df.rename(columns={'pm2_5': 'pm25_value'})

        def cal_aqi_target(pm25_value: float) -> str:
            if pm25_value <= 12.0:
                return 'low'
            elif pm25_value <= 35.4:
                return 'medium'
            elif pm25_value <= 55.4:
                return 'high'
            else:
                return 'hazardous'

        df['target_value'] = df['pm25_value'].apply(cal_aqi_target)

        # Append the target label to the end for the split component to read
        # Note: pm25_value is dropped to prevent data leakage
        final_cols.extend(["target_value"])
    else:
        # If live inference table has legacy columns, keep them out of final_cols
        pass

    # 3. Filter and impute numeric medians safely
    df_final_cols = df[final_cols]
    df_final = df_final_cols.fillna(df_final_cols.median(numeric_only=True))

    return df_final
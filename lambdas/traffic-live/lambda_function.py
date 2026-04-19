import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    # Yesterday's date
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    day = yesterday.strftime('%d')
    
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # URL of current year CSV
    url = f"https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031/download/sid_dav_verkehrszaehlung_miv_OD2031_{year}.csv"
    print(f"Downloading in chunks: {url}")
    
    # Read and filter in chunks, never loads full file into memory
    chunks = []
    for chunk in pd.read_csv(url, chunksize=10000):
        chunk['MessungDatZeit'] = pd.to_datetime(chunk['MessungDatZeit'], utc=True)
        filtered = chunk[(chunk['MessungDatZeit'] >= yesterday_start) & 
                         (chunk['MessungDatZeit'] <= yesterday_end)]
        if len(filtered) > 0:
            chunks.append(filtered)
    
    df_yesterday = pd.concat(chunks) if chunks else pd.DataFrame()
    print(f"Rows for yesterday: {len(df_yesterday)}")
    
    # Convert to CSV in memory
    csv_buffer = StringIO()
    df_yesterday.to_csv(csv_buffer, index=False)
    
    # Upload to S3
    s3 = boto3.client('s3')
    bucket = 'team-survival-traffic'
    key = f"raw/live/year={year}/month={month}/day={day}/traffic_{year}-{month}-{day}.csv"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    
    print(f"Uploaded to s3://{bucket}/{key}")
    
    return {
        'statusCode': 200,
        'body': f"Successfully uploaded {len(df_yesterday)} rows for {year}-{month}-{day}"
    }

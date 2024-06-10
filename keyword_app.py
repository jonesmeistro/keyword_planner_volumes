import streamlit as st
import pandas as pd
import time
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from datetime import datetime
from ratelimit import limits, sleep_and_retry

# Constants
BATCH_SIZE = 9999
MAX_KEYWORDS = 200000
LANGUAGE_CODE = "1000"
RETRY_LIMIT = 3
RETRY_DELAY = 3  # Delay between retries in seconds

# Path for country codes CSV
country_geo_targets_path = "country_geo_targets.csv"

# Load country geo targets data
country_geo_targets_df = pd.read_csv(country_geo_targets_path)

# Priority countries with criteria IDs
priority_country_criteria_ids = {
    "United Kingdom": 1002316,
    "United States": 1012873,
    "France": 1005781,
    "Germany": 1003853,
    "Italy": 1008021,
    "Spain": 1005402,
    "Netherlands": 1010304
}

# Create a list of all country names
all_countries = country_geo_targets_df['Name'].tolist()

# Combine priority countries with all other countries, ensuring no duplicates
priority_country_names = list(priority_country_criteria_ids.keys())
remaining_countries = [country for country in all_countries if country not in priority_country_names]
all_countries_sorted = priority_country_names + sorted(remaining_countries)

# Create a mapping from country names to criteria IDs
country_name_to_criteria_id = {
    row['Name']: row['Criteria ID'] for index, row in country_geo_targets_df.iterrows()
}

# Rate limit the function to 1 call per second (60 calls per minute)
@sleep_and_retry
@limits(calls=1, period=1)
def call_generate_historical_metrics(client, customer_id, keywords, geo_target, language_code):
    return generate_historical_metrics(client, customer_id, keywords, geo_target, language_code)

def generate_historical_metrics(client, customer_id, keywords, geo_target, language_code):
    googleads_service = client.get_service("GoogleAdsService", version="v14")
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService", version="v14")
    request = client.get_type("GenerateKeywordHistoricalMetricsRequest", version="v14")
    request.customer_id = customer_id
    request.keywords.extend(keywords)
    request.geo_target_constants.append(
        googleads_service.geo_target_constant_path(geo_target)
    )
    request.keyword_plan_network = (
        client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
    )
    request.language = googleads_service.language_constant_path(language_code)

    response = keyword_plan_idea_service.generate_keyword_historical_metrics(
        request=request
    )

    results = []
    for result in response.results:
        metrics = result.keyword_metrics
        monthly_search_volumes = {
            f"{m.month.name}-{m.year}": m.monthly_searches for m in metrics.monthly_search_volumes
        }
        result_dict = {
            "Keyword": result.text,
            "Monthly Search Estimated": metrics.avg_monthly_searches,
            "Competition Level": metrics.competition.name,
            "Top of Page Bid Low Range": metrics.low_top_of_page_bid_micros,
            "Top of Page Bid High Range": metrics.high_top_of_page_bid_micros,
        }
        result_dict.update(monthly_search_volumes)
        results.append(result_dict)

    return results

def initialize_google_ads_client():
    google_ads_config = {
        "developer_token": st.secrets["developer_token"],
        "client_id": st.secrets["client_id"],
        "client_secret": st.secrets["client_secret"],
        "refresh_token": st.secrets["refresh_token"],
        "login_customer_id" : st.secrets["login_customer_id"],
        "client_customer_id": st.secrets["client_customer_id"],
        "use_proto_plus" : "True"
    }
    return GoogleAdsClient.load_from_dict(google_ads_config)

def calculate_changes(df):
    month_columns = [col for col in df.columns if '-' in col]
    latest_month = month_columns[-1]
    oldest_month = month_columns[0]

    # Calculate 3 Month Change if there are at least 4 months of data
    if len(month_columns) >= 4:
        df['3 Month Change'] = ((df[latest_month] - df[month_columns[-4]]) / df[month_columns[-4]] * 100).round(2)
    else:
        df['3 Month Change'] = None

    # Calculate 12 Month Change if there are at least 12 months of data
    if len(month_columns) >= 12:
        df['12 Month Change'] = ((df[latest_month] - df[month_columns[-12]]) / df[month_columns[-12]] * 100).round(2)
    else:
        df['12 Month Change'] = None

    # Calculate month-to-month percentage changes
    for i in range(1, len(month_columns)):
        prev_month = month_columns[i - 1]
        curr_month = month_columns[i]
        df[f'Change_{prev_month}_to_{curr_month}'] = ((df[curr_month] - df[prev_month]) / df[prev_month] * 100).round(2)

    # Calculate the average 12-month change
    if len(month_columns) >= 12:
        change_columns_12_months = [f'Change_{month_columns[i - 1]}_to_{month_columns[i]}' for i in range(1, 12)]
        df['Average 12 Month Change'] = df[change_columns_12_months].mean(axis=1).round(2)
    else:
        df['Average 12 Month Change'] = None

    # Calculate the average 3-month change
    if len(month_columns) >= 3:
        change_columns_3_months = [f'Change_{month_columns[i - 1]}_to_{month_columns[i]}' for i in range(-3, 0)]
        df['Average 3 Month Change'] = df[change_columns_3_months].mean(axis=1).round(2)
    else:
        df['Average 3 Month Change'] = None

    # Drop the individual month-to-month change columns
    df.drop(columns=[col for col in df.columns if col.startswith('Change_')], inplace=True)

    return df

def log_missing_keywords(missing_keywords):
    with open('missing_keywords.log', 'a', encoding='utf-8') as f:
        for keyword in missing_keywords:
            f.write(f"{keyword}\n")

def process_batch(client, customer_id, keywords, geo_target, language_code):
    retry_count = 0
    while retry_count < RETRY_LIMIT:
        try:
            batch_results = call_generate_historical_metrics(client, customer_id, keywords, geo_target, language_code)
            return batch_results
        except GoogleAdsException as ex:
            retry_count += 1
            time.sleep(RETRY_DELAY)
            if retry_count == RETRY_LIMIT:
                raise ex

def process_keywords_in_batches(client, customer_id, keywords, geo_target, language_code):
    all_results = []
    total_keywords = len(keywords)
    
    for i in range(0, total_keywords, BATCH_SIZE):
        batch_keywords = keywords[i:i + BATCH_SIZE]
        batch_results = process_batch(client, customer_id, batch_keywords, geo_target, language_code)
        all_results.extend(batch_results)
        
        # Ensure all batch keywords are accounted for in the results
        result_keywords = {result['Keyword'] for result in batch_results}
        missing_batch_keywords = set(batch_keywords) - result_keywords
        if missing_batch_keywords:
            log_missing_keywords(missing_batch_keywords)
        
        # Wait 3 seconds before processing the next batch
        time.sleep(3)
    
    # Retry processing missing keywords
    missing_keywords = [keyword for keyword in keywords if keyword not in {result['Keyword'] for result in all_results}]
    if missing_keywords:
        for i in range(0, len(missing_keywords), BATCH_SIZE):
            batch_keywords = missing_keywords[i:i + BATCH_SIZE]
            batch_results = process_batch(client, customer_id, batch_keywords, geo_target, language_code)
            all_results.extend(batch_results)
            result_keywords = {result['Keyword'] for result in batch_results}
            remaining_missing_keywords = set(batch_keywords) - result_keywords
            if remaining_missing_keywords:
                log_missing_keywords(remaining_missing_keywords)
            time.sleep(3)

    return all_results

# Streamlit UI
st.title("Google Ads Keyword Data Fetcher")

# Initialize Google Ads client using Streamlit secrets
google_ads_client = initialize_google_ads_client()

with st.form(key='keyword_form'):
    keyword_text = st.text_area("Paste your keywords here (one per line):", height=200)
    keywords = [k.strip() for k in keyword_text.split('\n') if k.strip()]

    selected_country = st.selectbox("Country:", all_countries_sorted)
    submit_button = st.form_submit_button(label='Fetch Data')

if submit_button:
    if keywords and selected_country:
        if len(keywords) > MAX_KEYWORDS:
            st.error(f"Please limit the number of keywords to {MAX_KEYWORDS}.")
        else:
            geo_target = country_name_to_criteria_id[selected_country]
            try:
                data = process_keywords_in_batches(google_ads_client, client_customer_id, keywords, geo_target, LANGUAGE_CODE)
                
                # Check if any keywords are missing after all batches
                processed_keywords = {result['Keyword'] for result in data}
                missing_keywords = set(keywords) - processed_keywords
                if missing_keywords:
                    log_missing_keywords(missing_keywords)

                if data:
                    df = pd.DataFrame(data)
                    df = calculate_changes(df)

                    # Rename columns
                    df.rename(columns={
                        'text': 'Keyword',
                        'approximate_monthly_searches': 'Monthly Search Estimated',
                        'competition_level': 'Competition Level',
                        'top_of_page_bid_low_range': 'Top of Page Bid Low Range',
                        'top_of_page_bid_high_range': 'Top of Page Bid High Range'
                    }, inplace=True)

                    # Re-arrange columns
                    new_column_order = [
                        'Keyword', 'Monthly Search Estimated', 'Competition Level', 
                        'Top of Page Bid Low Range', 'Top of Page Bid High Range',
                        '3 Month Change', '12 Month Change', 
                        'Average 12 Month Change', 'Average 3 Month Change'
                    ] + [col for col in df.columns if '-' in col]

                    df = df[new_column_order]

                    st.success("Data fetched successfully!")
                    st.dataframe(df)
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name='keyword_data.csv',
                        mime='text/csv',
                    )
                else:
                    st.warning("No data returned for the specified keywords.")
            except GoogleAdsException as ex:
                st.error(f"Request failed: {ex.error.code().name}")
                for error in ex.failure.errors:
                    st.error(f"Error with message: {error.message}")
                    if error.location:
                        for field_path_element in error.location.field_path_elements:
                            st.error(f"On field: {field_path_element.field_name}")
    else:
        st.error("Please provide keywords and select a country.")

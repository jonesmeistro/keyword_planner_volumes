import streamlit as st
import pandas as pd
import time
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from io import StringIO

# Constants
CLIENT_CUSTOMER_ID = st.secrets["client_customer_id"]
LANGUAGE_CODE = "1000"
BATCH_SIZE = 9999
RETRY_WAIT_TIME = 30  # Extended wait time to avoid hitting API limits

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

def call_generate_historical_metrics(client, customer_id, keywords, geo_target, language_code):
    return generate_historical_metrics(client, customer_id, keywords, geo_target, language_code)

def generate_historical_metrics(client, customer_id, keywords, geo_target, language_code):
    googleads_service = client.get_service("GoogleAdsService", version="v17")
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService", version="v17")
    request = client.get_type("GenerateKeywordHistoricalMetricsRequest", version="v17")
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
            "3 Month Change": None,
            "12 Month Change": None,
            "Average 12 Month Change": None,
            "Average 3 Month Change": None,
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
        "login_customer_id": st.secrets["login_customer_id"],
        "use_proto_plus": "True"
    }
    return GoogleAdsClient.load_from_dict(google_ads_config)

def extract_keywords_from_text(keywords_text, num_keywords):
    keywords = keywords_text.split("\n")[:num_keywords]
    keywords = [kw.strip() for kw in keywords if kw.strip()]
    return keywords

def calculate_changes(df):
    month_columns = [col for col in df.columns if '-' in col]
    latest_month = month_columns[-1]

    if len(month_columns) >= 4:
        df['3 Month Change'] = ((df[latest_month] - df[month_columns[-4]]) / df[month_columns[-4]] * 100).round(2)
    else:
        df['3 Month Change'] = None

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
    change_columns_12_months = [f'Change_{month_columns[i - 1]}_to_{month_columns[i]}' for i in range(1, 12)]
    df['Average 12 Month Change'] = df[change_columns_12_months].mean(axis=1).round(2)

    # Calculate the average 3-month change
    change_columns_3_months = change_columns_12_months[-3:]
    df['Average 3 Month Change'] = df[change_columns_3_months].mean(axis=1).round(2)

    # Drop the individual month-to-month change columns
    df.drop(columns=change_columns_12_months, inplace=True)

    return df

# Split keywords into batches
def split_keywords_into_batches(keywords, batch_size):
    for i in range(0, len(keywords), batch_size):
        yield keywords[i:i + batch_size]

# Retry logic
def retry_missed_keywords(client, customer_id, missed_keywords, geo_target, language_code, max_attempts=3):
    attempt = 0
    remaining_keywords = missed_keywords
    processed_keywords = set()

    while remaining_keywords and attempt < max_attempts:
        attempt += 1
        st.write(f"Attempt {attempt} to process {len(remaining_keywords)} missed keywords...")
        keyword_batches = list(split_keywords_into_batches(remaining_keywords, BATCH_SIZE))

        for batch_num, batch in enumerate(keyword_batches, start=1):
            st.write(f"Processing retry batch {batch_num} of {len(keyword_batches)}...")
            try:
                data = call_generate_historical_metrics(client, customer_id, batch, geo_target, language_code)
                if data:
                    df = pd.DataFrame(data)
                    df = calculate_changes(df)

                    # Save the DataFrame to a CSV file
                    df.to_csv('C:\\Users\\cjones01\\Downloads\\keyword_data.csv', mode='a', header=False, index=False)
                    st.write(f"Data for {len(df)} keywords saved to CSV.")

                    # Update processed keywords and remaining keywords
                    processed_keywords.update(df['Keyword'].tolist())
                    remaining_keywords = [kw for kw in remaining_keywords if kw not in processed_keywords]
                else:
                    st.write(f"No data returned for retry batch {batch_num}.")
            except GoogleAdsException as ex:
                st.write(f"Request failed on attempt {attempt}, batch {batch_num}: {ex.error.code().name}")
                for error in ex.failure.errors:
                    st.write(f"Error with message: {error.message}")
                    if error.location:
                        for field_path_element in error.location.field_path_elements:
                            st.write(f"On field: {field_path_element.field_name}")

            if remaining_keywords:
                st.write(f"Waiting for {RETRY_WAIT_TIME} seconds before retrying...")
                time.sleep(RETRY_WAIT_TIME)

    return remaining_keywords

# Streamlit app
st.title('Keyword Historical Metrics Generator')

st.write("""
Enter your keywords (one per line) in the text box below. You can input up to 100,000 keywords. The app will process these keywords and provide historical metrics data.
""")

# Country selection dropdown
country = st.selectbox("Select a country:", all_countries_sorted)

keywords_text = st.text_area("Enter keywords here:", height=300)

# Text box for user messages
message_box = st.empty()

if st.button('Generate Metrics'):
    keywords = extract_keywords_from_text(keywords_text, 100000)
    total_keywords = len(keywords)

    if total_keywords == 0:
        st.error("Please enter at least one keyword.")
    else:
        google_ads_client = initialize_google_ads_client()
        message_box.write(f"Total keywords: {total_keywords}")

        batches = list(split_keywords_into_batches(keywords, BATCH_SIZE))
        total_batches = len(batches)
        message_box.write(f"Splitting into {total_batches} batches.")

        all_results = []
        processed_keywords = set()  # To keep track of all processed keywords
        not_pulled_keywords = []

        geo_target = country_name_to_criteria_id[country]

        for batch_number, batch in enumerate(batches, start=1):
            message_box.write(f"Processing batch {batch_number} of {total_batches}...")
            try:
                data = call_generate_historical_metrics(google_ads_client, CLIENT_CUSTOMER_ID, batch, geo_target, LANGUAGE_CODE)
                
                if data:
                    df = pd.DataFrame(data)
                    df = calculate_changes(df)

                    all_results.append(df)
                    batch_count = len(df)
                    message_box.write(f"Got data for {batch_count} keywords from batch {batch_number}.")

                    # Track processed keywords
                    processed_keywords.update(df['Keyword'].tolist())

                else:
                    message_box.write(f"No data returned for batch {batch_number}.")
                
                # Track missed keywords
                missed_keywords = [kw for kw in batch if kw not in processed_keywords]
                not_pulled_keywords.extend(missed_keywords)
                message_box.write(f"Missed {len(missed_keywords)} keywords in batch {batch_number}.")

            except GoogleAdsException as ex:
                message_box.write(f"Request failed: {ex.error.code().name}")
                for error in ex.failure.errors:
                    message_box.write(f"Error with message: {error.message}")
                    if error.location:
                        for field_path_element in error.location.field_path_elements:
                            message_box.write(f"On field: {field_path_element.field_name}")
                not_pulled_keywords.extend(batch)
                message_box.write(f"Batch {batch_number} failed.")

            # Wait for 5 seconds before the next batch
            message_box.write("Waiting for 5 seconds before the next batch...")
            time.sleep(5)

        # Retry missed keywords up to 2 more times
        message_box.write("Cleaning up and processing any missed keywords, this could take up to 2 minutes. Please standby.")
        not_pulled_keywords = retry_missed_keywords(google_ads_client, CLIENT_CUSTOMER_ID, not_pulled_keywords, geo_target, LANGUAGE_CODE, max_attempts=3)

        # Combine all results into a single DataFrame
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            # Reorder the columns
            cols = list(final_df.columns)
            reordered_cols = cols[:2] + ["3 Month Change", "12 Month Change", "Average 12 Month Change", "Average 3 Month Change"] + cols[2:4] + cols[6:]
            final_df = final_df[reordered_cols]
            csv_data = final_df.to_csv(index=False)
        else:
            csv_data = ""

        # Provide download button for the captured data CSV file
        st.download_button(
            label="Download Captured Data",
            data=csv_data,
            file_name='captured_keyword_data.csv',
            mime='text/csv'
        )

        message_box.write("Processing complete.")

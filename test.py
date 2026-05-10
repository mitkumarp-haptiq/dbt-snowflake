import requests
import json

# --- Configuration (Replace these placeholder values) ---
PLATFORM_HOST = "your-platform-host"
COMPANY_ID = "your_company_id"
BEARER_TOKEN = "your_bearer_token"  # All requests require a valid Bearer token

# --- Request Details ---
API_ENDPOINT = f"https://{PLATFORM_HOST}/connector-service/v1/companies/{COMPANY_ID}/connectors/instances/execute"

# Request Headers
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# Request Body (list_contacts operation for ActiveCampaign)
# The request body structure requires app_id, connector_instance_name, operation, and params.
payload = {
    "app_id": "my-app",
    "connector_instance_name": "snow-test",
    "operation": "list_contacts",  # Operation name specific to the connector
    "params": {
        "limit": 20,
        "offset": 0,
        "search": "john"  # Operation-specific parameters
    }
}

# --- Execute the Request ---
try:
    response = requests.post(API_ENDPOINT, headers=headers, json=payload)
    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

    # The response body contains the result object with the provider response data.
    response_data = response.json()

    print("Request successful.")
    print(f"Operation executed: {response_data.get('operation')}")
    print("\nResults (snippet):")
    # Pretty-print the provider response data (if successful)
    print(json.dumps(response_data.get('result'), indent=2))

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    # Handle potential API errors (e.g., failure response structure)
    if response is not None:
        error_data = response.json()
        print("\nAPI Error Details:")
        print(f"Success: {error_data.get('success')}")
        print(f"Error Code: {error_data.get('error_code')}")
        print(f"Error Message: {error_data.get('error')}")
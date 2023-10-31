import requests

api_key = ""

def test_ping():
    BASE_URL = "https://fapi.binance.com"
    ENDPOINT = "/fapi/v1/ping"
    
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {"X-MBX-APIKEY": api_key}
    
    response = requests.get(url, headers=headers)
    print(response.json())

def test_historical_trades(symbol, limit, from_id):
    url = "https://fapi.binance.com/fapi/v1/historicalTrades"
    headers = {"X-MBX-APIKEY": api_key}
    params = {"symbol": symbol, "limit": limit, "fromId": from_id}
    
    while True:
        response = requests.get(url, headers=headers, params=params)
        print(f"API call made with {params}")
        if response.status_code == 200:
            print("API call succeeded")
            #print(response.json())
            break
        elif response.status_code == 400:
            print("API call failed with status code 400, retrying with reduced limit")
            reduced_limit = int(params["limit"] * 0.8)
            if reduced_limit < 1:
                print("Limit reduced to less than 1, cannot reduce further")
                print(response.text)
                break
            params["limit"] = reduced_limit
        else:
            print(f"API call failed with status code {response.status_code}")
            print(response.text)
            break
# %%
symbol = "RNDRUSDT"
limit = 1000
from_id = 87638524
# %%
test_historical_trades(symbol, limit, from_id)
# %%
test_ping()
# %%

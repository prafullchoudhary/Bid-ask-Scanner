from playwright.sync_api import Playwright, sync_playwright
from urllib.parse import parse_qs, urlparse, quote
import pyotp
import requests
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import json
from datetime import datetime
import config
import asyncio
import ssl
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep
from concurrent.futures import ThreadPoolExecutor as executor
import math
import os

API_KEY = '15a1a0cc-90c3-4835-b00e-bf17aa26ae85'
SECRET_KEY = '0i29qafvdq'
RURL = 'https://127.0.0.1:5000/'
TOTP_KEY = "63C53NKN2UMC4L2NBKVZBP55F64HS4QI"
MOBILE_NO = '8516096467'
PIN = '240103'
# API_KEY = '48e868c9-d900-4893-b276-631242388286'
# SECRET_KEY = '680qjb5e6g'
# RURL = 'https://127.0.0.1:5000/'
# TOTP_KEY = "ZX2H3AI7FNNYTOTFMBSRWKLBXOBU3EB4"
# MOBILE_NO = '8828190833'
# PIN = '258724'

rurlEncode = quote(RURL, safe="")
AUTH_URL = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={RURL}'
api_version = '2.0'
# print(AUTH_URL)
# os._exit(0)

def run(playwright: Playwright):
    try:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        with page.expect_request(f"*{RURL}?code*") as request:
            page.goto(AUTH_URL)
            page.locator("#mobileNum").click()
            page.locator("#mobileNum").fill(MOBILE_NO)
            page.get_by_role("button", name="Get OTP").click()
            page.locator("#otpNum").click()
            page.locator("#otpNum").fill(pyotp.TOTP(TOTP_KEY).now())
            page.get_by_role("button", name="Continue").click()
            page.get_by_label("Enter 6-digit PIN").fill(PIN)
            page.get_by_role("button", name="Continue").click()
            page.wait_for_load_state()

        url = request.value.url
        # print(f"Redirect Url with code: {url}")
        parsed = urlparse(url)
        code = parse_qs(parsed.query)['code'][0]
        context.close()
        browser.close()
        # print(code)
        return code 
    except Exception as e:
        print(f'Error occurred in login: {e}')

def fetch_expiry():
    fileURL = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(fileURL)

    # Optional: convert the expiry column to datetime format
    symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).dt.date

    niftyDf = symboldf[(symboldf.instrument_type == 'OPTIDX') & (symboldf.tradingsymbol.str.startswith('NIFTY')) & (symboldf.exchange == 'NSE_FO')]

    expiryList = niftyDf['expiry'].unique().tolist()
    expiryList.sort()
    #print(expiryList)
    # currentExpDf = niftyDf[niftyDf.expiry == expiryList[0]]
    # currentExpDf['last_price'] = currentExpDf['last_price'].astype(float)
    return expiryList[0]
    
def option_chain(expiry,token):
    
    url = "https://api.upstox.com/v2/option/chain"

    params={
        'instrument_key': "NSE_INDEX|Nifty 50",
        'expiry_date': f'{expiry}'
    }
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.request("GET", url, headers=headers, params=params,timeout=1)
        response = response.text
        data = json.loads(response)
        option_chain = pd.DataFrame(data['data'])
        return option_chain['call_options'],option_chain['put_options']
    except requests.exceptions.Timeout:
        return option_chain(expiry,token)
    except requests.exceptions.RequestException as e:
        print('Error in getting Option Chain Data : ',e)
    
# def option_chain(expiry,token):
    
#     url = "https://api.upstox.com/v2/option/chain"

#     params={
#         'instrument_key': "NSE_INDEX|Nifty 50",
#         'expiry_date': f'{expiry}'
#     }
#     headers = {
#     'Content-Type': 'application/json',
#     'Accept': 'application/json',
#     'Authorization': f'Bearer {token}'
#     }
#     try:
#         response = requests.get(url, headers=headers, params=params)

#         data = json.loads(response.text)
#         option_chain = pd.DataFrame(data['data'])
#         return option_chain['call_options'],option_chain['put_options']
#     except Exception as e:
#         print(e)
    
def market_open(token):
    url = 'https://api.upstox.com/v2/market/status/NSE'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers)
    if response.json()['data']['status']=='NORMAL_OPEN':
        return True
    else:
        return False

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

ltp=0
async def fetch_market_data(intrument,token):
    global ltp
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    configuration.access_token = token

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": [intrument]
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)
            ltp=data_dict['feeds'][intrument]['ltpc']['ltp']
            # Print the dictionary representation
            # print(json.dumps(data_dict))


# Execute the function to fetch market data
# asyncio.run(fetch_market_data())
def run_websocket(intrument,token):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data(intrument,token))

def give_instrument(data):
    instruments=[]
    for i in data:
        if i['market_data']['ltp']<110 and i['market_data']['ltp']>85:
            instruments.append((i['instrument_key'],(i['market_data']['bid_qty']/i['market_data']['ask_qty']),i['market_data']['ltp']))
    final_i=instruments[0]
    for j in range(1,len(instruments)):
        if final_i[2]<instruments[j][2]:
            final_i=instruments[j]
    return final_i

def place_order(api,itoken,qty,buy_sell,otype="MARKET",price=0,trigger=0):
    bundle=math.ceil(qty/1800)
    order_ids=[]
    for i in range(bundle):
        if qty>1800:
            body = upstox_client.PlaceOrderRequest(1800, "I", "DAY", price, "string", itoken, otype, buy_sell, 0, trigger, False)
            order_ids.append((api.place_order(body, api_version)).data.order_id)
            qty-=1800
        else:
            body = upstox_client.PlaceOrderRequest(qty, "I", "DAY", price, "string", itoken, otype, buy_sell, 0, trigger, False)
            order_ids.append((api.place_order(body, api_version)).data.order_id)
    return order_ids

def check_status(order_id,token):
    url = 'https://api.upstox.com/v2/order/details'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    params = {'order_id': order_id}
    response = requests.get(url, headers=headers, params=params)
    open=json.loads(response.text)
    return open['data']['status']

pos_l=0
def buy_price(api,placed_instrument):
    global pos_l
    if pos_l<50:
        positions=api.get_positions(api_version)
        pos_l+=1
        for p in positions.data:
            if p.instrument_token==placed_instrument:
                return p.buy_price
        sleep(0.043479)
        return buy_price(api,placed_instrument)
    
with sync_playwright() as playwright:
    code = run(playwright)

api_instance = upstox_client.LoginApi()
client_id = API_KEY
client_secret = SECRET_KEY
redirect_uri = RURL
grant_type = 'authorization_code'

try:
    api_response = api_instance.token(api_version, code=code, client_id=client_id, client_secret=client_secret,
                                      redirect_uri=redirect_uri, grant_type=grant_type)
    # print(api_response.access_token)
except ApiException as e:
    print("Exception when calling LoginApi->token: %s\n" % e)
    
configuration = upstox_client.Configuration()
configuration.access_token = api_response.access_token
# configuration.access_token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJKUDIwMDUiLCJqdGkiOiI2NWUxNmU3MTQ1NzRjZDA5NDI0YWQ1YjQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNBY3RpdmUiOnRydWUsInNjb3BlIjpbImludGVyYWN0aXZlIiwiaGlzdG9yaWNhbCJdLCJpYXQiOjE3MDkyNzI2ODksImlzcyI6InVkYXBpLWdhdGV3YXktc2VydmljZSIsImV4cCI6MTcwOTMzMDQwMH0.o-HsNLO54sliaH0MTe__Kvsf0JUFU7qHb41Ji963l3A'

api_instance = upstox_client.UserApi(upstox_client.ApiClient(configuration))
order_api=upstox_client.OrderApi(upstox_client.ApiClient(configuration))
position = upstox_client.PortfolioApi(upstox_client.ApiClient(configuration))

def fund(api):
    try:
        funds = (api.get_user_fund_margin(api_version,segment='SEC').data['equity'].available_margin)*0.75
        return funds
    except ApiException as e:
        print("Exception when calling UserApi->get_user_fund_margin: %s\n" % e)

# while market_open(configuration.access_token)==True:

if market_open(configuration.access_token):
# if True:
    expiry=fetch_expiry()
    if (expiry!=datetime.now().date()) and (datetime.today().weekday() != 5) and (datetime.today().weekday() != 6) :
        print('Running...')
        while True:
            if (datetime.strptime(config.time, "%H:%M").time()<=datetime.now().time()):
                call_data,put_data=option_chain(expiry,configuration.access_token)
                call = executor().submit(give_instrument,call_data)
                put = executor().submit(give_instrument,put_data)
                funds=fund(api_instance)
                ce_instument,ce_bs_ratio,ce_ltp=call.result()
                pe_instument,pe_bs_ratio,pe_ltp=put.result()
                if pe_bs_ratio>=ce_bs_ratio:
                    qty=int((funds/pe_ltp)/50)*50
                    if qty>0:
                        place_order(order_api,pe_instument,qty,'BUY')
                        placed_instrument=pe_instument
                    else:
                        print(f'Funds insufficient to buy atleast 50 QTY: {funds}(NOTE: This is 75% of your total available funds)')
                        break
                else:
                    qty=int((funds/ce_ltp)/50)*50
                    if qty>0:
                        place_order(order_api,ce_instument,qty,'BUY')
                        placed_instrument=ce_instument 
                    else:
                        print(f'Funds insufficient to buy atleast 50 QTY: {funds}(NOTE: This is 75% of your total available funds)')
                        break
                print('Order placed')
                executor().submit(run_websocket,placed_instrument,configuration.access_token)
                ibuy_price=buy_price(position,placed_instrument)
                sl=round((int((ibuy_price*(1-config.stop_loss_percent)) / 0.05) * 0.05),2)
                stop_order=place_order(order_api,placed_instrument,qty,'SELL',otype="SL",price=sl,trigger=sl+0.05)
                print('SL placed')
                tp=ibuy_price*(1+config.target_percent)
                while True:
                    if ltp!=0:
                        break
                previous_ltp=-1
                print('Checking target and SL conditions...')
                while True:
                    if previous_ltp!=ltp:
                        if ltp<=sl:
                            if check_status(stop_order[-1],configuration.access_token)=='complete':
                                print('SL hit')
                                break
                        elif ltp>tp:
                            if check_status(stop_order[-1],configuration.access_token)!='complete':
                                for s in stop_order:
                                    order_api.cancel_order(s, api_version)
                                print('SL order cancelled')
                                place_order(order_api,placed_instrument,qty,'SELL')
                                print('Target achieved')
                            break
                        previous_ltp=ltp
                    sleep(0.001)
                break
    else:
        print('Strategy will not run today')
else:
    print('Market is closed today')

os._exit(0)

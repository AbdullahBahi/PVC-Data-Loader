import datetime
import time
import pandas as pd
from argparse import ArgumentParser
from pycoingecko import CoinGeckoAPI
from os import getcwd
from os.path import join

def convert_seconds(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
      
    return "%d:%02d:%02d" % (hour, minutes, seconds)

def get_weekly_market_cap_by_id(coin_id, min_weeks=200):
    caps = pd.DataFrame(cg.get_coin_market_chart_by_id(coin_id, 'usd',min_weeks*7,interval='daily')['market_caps'])
    caps.columns = ['Date', 'Cap']
    caps.index = pd.to_datetime([datetime.datetime.fromtimestamp(d / 1e3) for d in caps['Date']]).round('s')  
    caps['week_id'] = caps.index.isocalendar().year.astype(str).str.cat(caps.index.isocalendar().week.astype(str), sep='_')  
    caps.drop(['Date'], axis=1, inplace=True)
    caps = caps.groupby(['week_id']).last()
    caps['asset'] = [coin_id]*len(caps)
    if len(caps) < min_weeks:
        return
    else:
        return caps

def get_daily_market_cap_by_id(coin_id, min_days=200):
    caps = pd.DataFrame(cg.get_coin_market_chart_by_id(coin_id, 'usd',min_days,interval='daily')['market_caps'])
    caps.columns = ['Date', 'Cap']
    caps.index = pd.to_datetime([datetime.datetime.fromtimestamp(d / 1e3) for d in caps['Date']]).round('s')  
    caps['asset'] = [coin_id]*len(caps)
    if len(caps) < min_days:
        return
    else:
        return caps

def get_pvc_by_id(coin_id, n=None, freq=None):
    if n is None:
        n = 'max'
    if freq is None:
        freq = 'daily'
    p = pd.DataFrame(cg.get_coin_market_chart_by_id('01coin', 'usd', n,interval=freq)['prices'])
    v = pd.DataFrame(cg.get_coin_market_chart_by_id('01coin', 'usd', n,interval=freq)['total_volumes'])
    c = pd.DataFrame(cg.get_coin_market_chart_by_id('01coin', 'usd', n,interval=freq)['market_caps'])
    pvc = pd.concat([p,v,c], axis=1)
    pvc.columns = ['Date', 'Price', 'to_drop', 'Volume', 'to_drop', 'Market_Cap']
    pvc.drop(['to_drop'], axis=1, inplace=True)
    pd.to_datetime([datetime.datetime.fromtimestamp(d / 1e3) for d in pvc['Date']]).round('s') 
    pvc.set_index(['Date'], inplace=True)
    pvc['Asset'] = [coin_id]*len(pvc)
    return pvc

if __name__ == '__main__':
    CWD = getcwd()
    ## Parse input args
    parser = ArgumentParser()
    parser.add_argument('-f', '--freq', default='daily', type=str, help='frequency of data [daily/weekly]')
    parser.add_argument('-n', '--num_data_points', default='max', type=str, help='Number of data points to be downloaded')
    args = parser.parse_args()

    ## Initialize CoinGecko instance
    cg = CoinGeckoAPI()

    ## Git a list oc supported coins (without tokens)
    coins = pd.read_csv('coins.csv').ID.to_list()

    ## Initialize variables for download loop
    PVC = pd.DataFrame()
    coin_counter = 0
    checkpoint_counter = 0
    start = time.time()

    try:
        print('Downloading data ..')
        for idx, coin_id in enumerate(coins):
            # if idx < 1:
            #     continue
            coin_counter += 1
            try:
                pvc = get_pvc_by_id(coin_id, n=args.num_data_points, freq=args.freq)
            except:
                finished_sleeping = False
                print(f'\nPrices, Volumes and Market Caps for {coin_counter} Coins have been downloaded successfully!')
                print('Minutely Rate limit has been reached! sleeping until next call ..')
                while not finished_sleeping:    
                    try:
                        pvc = get_pvc_by_id(coin_id, n=args.num_data_points, freq=args.freq)
                        print('Done sleeping, Downloading data ..\n')
                        finished_sleeping = True
                    except:
                        time.sleep(1)
            if pvc is not None:
                PVC = pd.concat([PVC, pvc])
            
            if coin_counter % 100 == 0:
                end = time.time()
                print(f'\n{round((coin_counter/len(coins))*100, 2)}% is finished!')
                print(f'Elapsed Time : {convert_seconds(end-start)}')
                print(f'Estimated Remaining Time : {convert_seconds(((end-start)/coin_counter)*(len(coins)-coin_counter))}')
                print(f'{checkpoint_counter} Checkpoints are saved!\n')
            
            if len(PVC) > 500000:
                checkpoint_counter += 1
                PVC.to_excel(join(CWD, 'data','pvc_'+str(checkpoint_counter)+'.xlsx'))
                PVC = pd.DataFrame()
                print(f'\n{checkpoint_counter} Checkpoints are saved!')

        checkpoint_counter += 1
        PVC.to_excel(join(CWD, 'data','pvc_'+str(checkpoint_counter)+'.xlsx'))
    except:
        PVC.to_excel(join(CWD, 'data','pvc_'+str(checkpoint_counter)+'.xlsx'))
        print(f'last index is {idx}')
    else:
        PVC.to_excel(join(CWD, 'data','pvc_'+str(checkpoint_counter)+'.xlsx'))
        print(f'All Data is downloaded Successfully! last index is {idx}')
        ## this should print 6259


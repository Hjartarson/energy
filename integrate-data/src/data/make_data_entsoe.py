import sys
import os
import pandas as pd
import time
from entsoe import EntsoePandasClient

def save_data(filename, dataframe, append = True):
    if os.path.isfile(filename):
        df_stored = pd.read_pickle(filename, compression = 'gzip')
        dataframe = dataframe.combine_first(df_stored)
        dataframe.to_pickle(filename, compression = 'gzip')
    else:
        dataframe.to_pickle(filename, compression = 'gzip')
    # Sleep and avoid api-disconnect
    time.sleep(0.1)

def hourly_resample_and_make_unique(data):
    data.index = data.index.tz_localize(None)
    data = data.resample('H', label='left').mean()
    data = data.groupby(data.index).mean()
    return data

def main(method, date_from, date_to):
    """ Main program """

    BZ_1H = ['SE_1', 'SE_2', 'SE_3', 'SE_4', 'DK_1', 'DK_2', 'FR', 'LT', 'LV', 'EE', 'FI', 'NO_1', 'NO_2', 'NO_3', 'NO_4', 'PL']
    BZ_COUNTRY = ['SE', 'DK', 'NO']
    BZ_OTHER = ['CZ', 'SI', 'BG', 'HR', 'HU', 'GR', 'IT', 'ES', 'CH']
    BZ_SE = ['SE_1', 'SE_2', 'SE_3', 'SE_4']
    BIDDING_ZONES_15MIN = ['NL', 'BE', 'AT', 'DE_LU']

    BZ_ALL = BZ_1H + BZ_COUNTRY + BZ_OTHER + BIDDING_ZONES_15MIN
    BZ_ALL = BZ_SE + ['SE']
    BZ_ALL = ['SE_4', 'FR', 'DE_LU', 'NL', 'BE', 'PL', 'IT']
    
    fileObject = open("entsoe_api.txt", "r")
    api_key = fileObject.read()
    
    client = EntsoePandasClient(api_key = api_key)
    start = pd.Timestamp(date_from, tz='Europe/Stockholm')
    end = pd.Timestamp(date_to, tz='Europe/Stockholm')

    
    if method == 'load':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_load(bz, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            data = data.rename(columns={'Actual Load':bz})
            save_data('../../data/df_load.pkl', data)
        
    if method == 'net_position':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_net_position(bz, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            data = data.rename(bz).to_frame()
            save_data('../../data/df_net_position.pkl', data)
        
    if method == 'load_day_ahead':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_load_forecast(bz, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            data = data.rename(columns={'Forecasted Load':bz})
            save_data('../../data/df_load_day_ahead.pkl', data)
        
    if method == 'generation_day_ahead':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_generation_forecast(bz, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            if isinstance(data, pd.DataFrame):
                data = data['Actual Aggregated'].rename(bz)
            else:
                data = data.rename(bz)
            data = data.to_frame()
            save_data('../../data/df_generation_day_ahead.pkl', data)
        
    if method == 'price_day_ahead':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_day_ahead_prices(bz, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            data = data.rename(bz).to_frame()
            save_data('../../data/df_price_day_ahead.pkl', data)

    if method == 'water_storage':
        for bz in BZ_ALL:
            print(bz)
            data = client.query_aggregate_water_reservoirs_and_hydro_storage(bz, start=start, end=end)
            data.index = data.index.tz_localize(None)
            data = data.groupby(data.index).mean()
            data = data.rename(bz).to_frame()
            save_data('../../data/df_water_storage.pkl', data)
            

    if method == 'generation_type':
        for bz in BZ_ALL:
            print(bz)
            df_gen = client.query_generation(bz, start=start, end=end)
            df_gen = hourly_resample_and_make_unique(df_gen)
            for col in df_gen.columns:
                print(col)
                data = df_gen[col]
                data = data.rename((bz, col)).to_frame()
                save_data('../../data/df_generation_type.pkl', data)


    
    if method == 'flow_day_ahead':
    
        code_flow = [('SE_1', 'SE_2'), ('SE_2', 'SE_3'), ('SE_3', 'SE_4'), 
                     ('SE_1', 'NO_4'), ('NO_4', 'SE_1'), ('SE_2', 'NO_4'), ('NO_4', 'SE_2'), ('SE_2', 'NO_3'), ('NO_3', 'SE_2'), 
                     ('SE_1', 'FI'), ('FI', 'SE_1'), ('SE_3', 'FI'), ('FI', 'SE_3'), 
                     ('SE_3', 'DK_1'), ('DK_1', 'SE_3'), ('SE_4', 'DK_2'), ('DK_2', 'SE_4'), 
                     ('SE_4', 'LT'), ('LT', 'SE_4'),
                     ('FI', 'EE'), ('EE', 'FI'),
                     ('SE_4', 'DE_LU'), ('DE_LU', 'SE_4'),
                     ('SE_4', 'PL'), ('PL', 'SE_4'),
                     ('LT', 'LV'), ('LV', 'LT'), ('LV', 'EE'), ('EE', 'LV')]
                     
        code_flow_se = [('SE_1', 'SE_2'), ('SE_2', 'SE_3'), ('SE_3', 'SE_4')]
    

        for code in code_flow_se:
            bz_from = code[0]
            bz_to = code[1]
            print(bz_from, bz_to)
            data = client.query_scheduled_exchanges(bz_from, bz_to, start=start, end=end)
            data = hourly_resample_and_make_unique(data)
            data = data.rename((bz_from, bz_to)).to_frame()
            save_data('../../data/df_flow_day_ahead.pkl', data)
    
    if method == 'capacity_day_ahead':

        code_flow = [('SE_1', 'SE_2'), ('SE_2', 'SE_3'), ('SE_3', 'SE_4'), 
                     ('SE_1', 'NO_4'), ('NO_4', 'SE_1'), ('SE_2', 'NO_4'), ('NO_4', 'SE_2'), ('SE_2', 'NO_3'), ('NO_3', 'SE_2'), 
                     ('SE_1', 'FI'), ('FI', 'SE_1'), ('SE_3', 'FI'), ('FI', 'SE_3'), 
                     ('SE_3', 'DK_1'), ('DK_1', 'SE_3'), ('SE_4', 'DK_2'), ('DK_2', 'SE_4'), 
                     ('SE_4', 'LT'), ('LT', 'SE_4'),
                     ('FI', 'EE'), ('EE', 'FI'),
                     ('SE_4', 'DE_LU'), ('DE_LU', 'SE_4'),
                     ('SE_4', 'PL'), ('PL', 'SE_4'),
                     ('LT', 'LV'), ('LV', 'LT'), ('LV', 'EE'), ('EE', 'LV')]
                     
        code_flow_se = [('SE_1', 'SE_2'), ('SE_2', 'SE_3'), ('SE_3', 'SE_4')]

        for code in code_flow_se:
            bz_from = code[0]
            bz_to = code[1]
            print(bz_from, bz_to)

            data = client.query_offered_capacity(bz_from, bz_to, start=start, end=end, contract_marketagreement_type='A01')
            data = hourly_resample_and_make_unique(data)
            data = data.rename((bz_from, bz_to)).to_frame()
            save_data('../../data/df_capacity_day_ahead.pkl', data)
    
    
    return 0
    
    
    

if __name__ == "__main__":

    method = sys.argv[1]
    date_from = sys.argv[2]
    date_to = sys.argv[3]
    main(method, date_from, date_to)
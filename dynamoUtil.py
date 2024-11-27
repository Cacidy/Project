import datetime

import boto3
import pandas as pd
import time
import calendar
from boto3.dynamodb.conditions import Key, Attr

from src.keys import aws_keys as key
from src.static import constants as cst
from src.mapping import ACCOUNT_DICT


def dynamodb_init(env='live'):
    if env == 'live':
        return boto3.resource(
            cst.DB_DYNAMODB,
            aws_access_key_id=key.aws_access_key_id_casval_db,
            aws_secret_access_key=key.aws_secret_access_key_casval_db,
            region_name=key.region
        )
    elif env == 'uat':
        return boto3.resource(
            cst.DB_DYNAMODB,
            aws_access_key_id=key.aws_access_key_id_casval_db_uat,
            aws_secret_access_key=key.aws_secret_access_key_casval_db_uat,
            region_name=key.region
        )
    else:
        return None

def _generate_filter_expression( filter_key, filter_operation, filter_key_start_value, filter_key_end_value='' ):
    ''' Generate filter expression to be used by DynamoDB query. Currently support equal and between only, can be extended.
    
     Args:
         filter_key(str): filter table attribute. Example: 'amount'
         filter_operation(str): filter operation. Enum: [ 'eq', 'between' ]
         filter_key_start_value(str): filter start value.
         filter_key_end_value(str): filter end value.

     Return:
         FilterExpression
    '''
    if filter_operation == 'eq':
        return Attr(filter_key).eq(filter_key_start_value) 
    if filter_operation == 'between':
        return Attr(filter_key).between(filter_key_start_value, filter_key_end_value)
    

def single_query(table_name, index_name='', primary_key='', primary_key_value='', 
                 sorting_key='', sorting_key_start_value='', sorting_key_end_value='', 
                 filter_key='', filter_operation='', filter_key_start_value='', filter_key_end_value='',
                 exclusive_start_key='', env='live'):
    params = {}
    dynamodb = dynamodb_init(env)
    table = dynamodb.Table(table_name)
    
    key_condition_expression = Key(primary_key).eq(primary_key_value)
    if sorting_key:
        if not sorting_key_end_value:
            sorting_key_end_value = sorting_key_start_value
        sorting_key_condition_expression = Key(sorting_key).between(sorting_key_start_value, sorting_key_end_value)
        key_condition_expression = key_condition_expression & sorting_key_condition_expression
    params[ 'KeyConditionExpression' ] = key_condition_expression
    
    if filter_key:
        filter_expression = _generate_filter_expression( filter_key, filter_operation, filter_key_start_value, filter_key_end_value )
        params[ 'FilterExpression' ] = filter_expression
        
    if index_name:
        params[ 'IndexName' ] = index_name
        
    if exclusive_start_key:
        params[ 'ExclusiveStartKey' ] = exclusive_start_key
    
    response = table.query(**params)
    return response


def query(table_name, index_name='', primary_key='', primary_key_value='', 
          sorting_key='', sorting_key_start_value='', sorting_key_end_value='', 
          filter_key='', filter_operation='', filter_key_start_value='', filter_key_end_value='',
          exclusive_start_key='', env='live'):
    response = single_query(table_name=table_name, index_name=index_name, primary_key=primary_key, primary_key_value=primary_key_value, 
                            sorting_key=sorting_key, sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, 
                            filter_key=filter_key, filter_operation=filter_operation, filter_key_start_value=filter_key_start_value, filter_key_end_value=filter_key_end_value,
                            env=env)
    output = response['Items']
    while 'LastEvaluatedKey' in list(response):
        exclusive_start_key = response['LastEvaluatedKey']
        response = single_query(table_name=table_name, index_name='', primary_key=primary_key, primary_key_value=primary_key_value, 
                                sorting_key=sorting_key, sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, 
                                filter_key=filter_key, filter_operation=filter_operation, filter_key_start_value=filter_key_start_value, filter_key_end_value=filter_key_end_value,
                                exclusive_start_key=exclusive_start_key, env=env)
        output.extend(response['Items'])
    return output


def price_bar_query(primary_key_value, sorting_key_start_value, sorting_key_end_value='', env='live'):
    '''Query price bar data

     Args:
         primary_key_value(str):instrumenPeriod. Example:'LINKUSDT-BMX-00000000-FUT|1'
         sorting_key_start_value(str):start time. Example:'2020-10-26-07-00'
         sorting_key_end_value(str):end date. Example:'2020-10-26-08-00'
         env('live'):environment. Example:'live'

     Return:
         price bar dataframe
     '''
    
    table_name = env + '.market.priceBars'
    primary_key = 'instrumentPeriod'
    sorting_key = 'timestamp'

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                    sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)
    response_df = pd.DataFrame(response)

    if not response_df.empty:
        response_df['timestamp'] = response_df['timestamp'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d-%H-%M'))
        response_df['instrument'] = response_df['instrumentPeriod'].apply(
            lambda x: x.split('|')[0])
        response_df.drop('instrumentPeriod', axis=1, inplace=True)
    return response_df


def market_fundings_query(primary_key_value, sorting_key_start_value, sorting_key_end_value='', env='live'):
    '''Download market funding data

     Args:
         primary_key_value(str):primary key. Example:'LINKUSDT-BMX-00000000-FUT|60'
         sorting_key_start_value(str):start time. Example:'2020-10-26-07-00'
         sorting_key_end_value(str):end date. Example:'2020-10-26-08-00'
         env('live'):environment. Example:'live'

     Return:
         market funding dataframe
     '''

    table_name = env + '.market.fundings'
    primary_key = 'instrumentPeriod'
    sorting_key = 'timestamp'

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                    sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)
    response_df = pd.DataFrame(response)
    response_df['instrument'] = response_df['instrumentPeriod'].apply(
        lambda x: x.split('|')[0])
    response_df.drop('instrumentPeriod', axis=1, inplace=True)
    return response_df

def market_fundings_confirmed_query(primary_key_value, sorting_key_start_value, sorting_key_end_value='', env='live'):
    '''Download market confirmed funding data. Timestamp is confirmed time, not payout time.

     Args:
         primary_key_value(str):primary key. Example:'LINKUSDT-BMX-00000000-FUT'
         sorting_key_start_value(str):start time. Example:'2020-10-26|04' or '2020-10-26'
         sorting_key_end_value(str):end date. Example:'2020-10-26|12' or '2020-10-27'
         env('live'):environment. Example:'live'

     Return:
         market confirmed funding funding dataframe
     '''

    table_name = env + '.market.fundings.confirmed'
    primary_key = 'instrument'
    sorting_key = 'dateHour'

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                    sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)
    response_df = pd.DataFrame(response)
    return response_df

def market_openInterests_query(primary_key_value, sorting_key_start_value, sorting_key_end_value='', env='live'):
    '''Download open interests data. Timestamp is confirmed time, not payout time.

     Args:
         primary_key_value(str):primary key. Example:'LINKUSDT-BMX-00000000-FUT'
         sorting_key_start_value(str):start time. Example:'2020-10-26|04' or '2020-10-26'
         sorting_key_end_value(str):end date. Example:'2020-10-26|12' or '2020-10-27'
         env('live'):environment. Example:'live'

     Return:
         market open interests dataframe
     '''

    table_name = env + '.market.openInterests'
    primary_key = 'instrument'
    sorting_key = 'timestamp'

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                    sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)
    response_df = pd.DataFrame(response)
    return response_df    

def intraday_positions_query(primary_key_value, env='live'):
    '''Download instraday position data. Primary key is timestamp by hour.

     Args:
         primary_key_value(str):primary key. Example:'2022-03-17-15'
         env('live'):environment. Example:'live'

     Return:
         intraday position dataframe with account, amount and isLong
     '''
    
    table_name = env + '.portfolio.intraday.positions'
    primary_key = 'intraday'

    response = query(table_name=table_name, primary_key=primary_key,
                     primary_key_value=primary_key_value, env=env)
    response_df = pd.DataFrame(response)
    if not response_df.empty:
        response_df['timestamp'] = response_df['intraday'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d-%H'))
        response_df['amount'] = response_df['amount'].apply(lambda x: float(x))
        response_df['account'] = response_df['accountInstrumentExchangeType'].apply(
            lambda x: x.split('|')[0])
        response_df['instrument'] = response_df['accountInstrumentExchangeType'].apply(
            lambda x: x.split('|')[1])
        response_df['type'] = response_df['accountInstrumentExchangeType'].apply(
            lambda x: x.split('|')[2])
        response_df['exchange'] = response_df['instrument'].apply(
            lambda x: x.split('-')[1])
        response_df.drop(
            ['intraday', 'accountInstrumentExchangeType'], axis=1, inplace=True)
        response_df = response_df[['account', 'instrument', 'exchange',
                                'amount', 'isLong', 'leverage', 'timestamp', 'type', 'updatedAt']]
    return response_df


def intraday_prices_query(primary_key_value, sorting_key_start_value='', sorting_key_end_value='', env='live' ):
    '''Download instraday prices data. Primary key is timestamp by hour.

     Args:
         primary_key_value(str):primary key. Example:'2022-03-17-15'
         sorting_key_start_value:instrument name. Example:'WAVESUSDT-OKX-00000000-FUT' or 'WAVESUSDT-OKX-SPOT'
         sorting_key_end_value:instrument name. Example:'WAVESUSDT-OKX-00000000-FUT' or 'WAVESUSDT-OKX-SPOT'
         env('live'):environment. Example:'live'

     Return:
         intraday prices dataframe with mid, ask, bid
     '''
    table_name = env + '.market.intraday.prices'
    primary_key = 'intraday'
    sorting_key = 'instrument'

    response = query(table_name=table_name, primary_key=primary_key, sorting_key=sorting_key,
                     sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, 
                     primary_key_value=primary_key_value, env=env)
    
    response_df = pd.DataFrame(response)
    if not response_df.empty:         
        response_df['timestamp'] = response_df['intraday'].apply(
            lambda x: datetime.datetime.strptime(x, '%Y-%m-%d-%H'))
        response_df['mid'] = response_df['mid'].apply(lambda x: float(x))
        response_df['exchange'] = response_df['instrument'].apply(
            lambda x: x.split('-')[1])
        response_df.drop('intraday', axis=1, inplace=True)
    return response_df


def trades_query(primary_key_value, sorting_key_start_value='', sorting_key_end_value='', 
                 filter_key='', filter_operation='', filter_key_start_value='', filter_key_end_value='', env='live'):
    '''Trade query. Primary key is account.

     Args:
         primary_key_value(str):primary key. Example:'DRB-53607'
         sorting_key_start_value(str):timestampInstrumentExchangeTypeId. Example:'1577006471683|ETHUSD-DRB-00000000-FUT|FUT|ETH-10624602'
         sorting_key_end_value(str):timestampInstrumentExchangeTypeId. Example:'1577006471683|ETHUSD-DRB-00000000-FUT|FUT|ETH-10624602'
         filter_key(str): filter table attribute. Example: 'amount'
         filter_operation(str): filter operation. Enum: [ 'eq', 'between' ]
         filter_key_start_value(str): filter start value.
         filter_key_end_value(str): filter end value.
         env('live'):environment. Example:'live'

     Return:
         intraday trades dataframe
     '''
         
    table_name = env + '.portfolio.trades'
    primary_key = 'account'
    sorting_key = 'timestampInstrumentExchangeTypeId'

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                     sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, 
                     filter_key=filter_key, filter_operation=filter_operation, filter_key_start_value=filter_key_start_value, filter_key_end_value=filter_key_end_value, env=env)
    response_df = pd.DataFrame(response)

    if not response_df.empty:
        response_df['updatedAt'] = response_df['updatedAt'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
        response_df['timestamp'] = response_df['timestampInstrumentExchangeTypeId'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(int(x.split('|')[0])/1000).strftime('%Y-%m-%d %H:%M:%S'))
        response_df['instrument'] = response_df['timestampInstrumentExchangeTypeId'].apply(
            lambda x: x.split('|')[1])
        response_df['exchangeType'] = response_df['timestampInstrumentExchangeTypeId'].apply(
            lambda x: x.split('|')[2])
        response_df['id'] = response_df['timestampInstrumentExchangeTypeId'].apply(
            lambda x: x.split('|')[3])
    return response_df



def order_audit_query(primary_key_value, sorting_key_start_value, env='live'):    
    table_name = env + '.portfolio.orderAudits'
    primary_key = 'accountInstrument'
    sorting_key = 'id'
    sorting_key_end_value = sorting_key_start_value

    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key,
                    sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)
    response_df = pd.DataFrame(response)
    if not response_df.empty:
        return response_df['user'][0]
    else:
        return 'NA'


def instrument_query(primary_key_value, sorting_key_start_value='', env='live'):
    '''Download instrument static info.

     Args:
         primary_key_value(str):exchange. Example:'OKX' or 'HBG'
         sorting_key(str):instrument name. Example:'BTCUSDT-OKX-00000000-FUT' or empty
         env('live'):environment. Example:'live'

     Return:
         instrument list dataframe
     '''    
    table_name = env + '.static.instruments'
    primary_key = 'exchange'
    sorting_key = 'id'
    if not sorting_key_start_value:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, env=env)
    else:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key, sorting_key_start_value=sorting_key_start_value, env=env)

    response_df = pd.DataFrame(response)
    return response_df


def portfolio_account_query(primary_key_value):
    '''Portfolio account mappings.

     Args:
         primary_key_value(str):portfolio. Example:'-MEvie6Wy6mmUgcM6kUp'

     Return:
         List of accounts under porfolio
     '''        
    table_name = 'live.portfolio.accounts'
    index_name = 'portfolio-index'
    primary_key = 'portfolio'
    
    response = query(table_name=table_name, index_name=index_name, primary_key=primary_key, primary_key_value=primary_key_value)

    return [account['id'] for account in response]


def realizedvols_query( instrument, period='1d', sorting_key_start_value='', sorting_key_end_value='', env='live' ):
    table_name = env + '.market.realizedVols'
    primary_key = 'instrumentConfig'
    sorting_key = 'timestamp'
    
    if period not in [ '1d', '3d', '7d', '30d']:
        return "Wrong period input, Only [ 1d, 3d, 7d, 30d ] is allowed."
    
    period_mapping = { '1d': '5-288',
                       '3d': '5-864',
                       '7d': '5-2016',
                       '30d': '60-720' }

    primary_key_value = '|'.join( [ instrument, period_mapping[ period ] ] )
    
    if not sorting_key_start_value or not sorting_key_end_value:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, env=env)
    else:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key, sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, env=env)

    response_df = pd.DataFrame(response)
    return response_df

def users_scan( env="live" ):
    table_name = env + '.static.users'
    dynamodb = dynamodb_init(env)
    table = dynamodb.Table(table_name)
    
    response = pd.DataFrame(table.scan()['Items'])

    return response

def intraday_price_impliedvols_query(primary_key_value, instrument = '', strike = 0, env='live'):
    '''Query Intraday Price Implied Vols. Primary key is intraday timestamp.

     Args:
         primary_key_value(str):intraday. Example:'2022-09-12-09'
         sorting key(str):instrument and strike. Example:'BTCUSD-DRB-20220913-FUT|23000' or empty
         env('live'):environment. Example:'live'

     Return:
         intraday price implied vols dataframe
     '''    

    table_name = env + '.market.intraday.priceImpliedVols'
    primary_key = 'intraday'
    sorting_key = 'instrumentStrike'

    sorting_key_value = instrument + '|' + str(strike) if (instrument and strike) else ''

    if not sorting_key_value:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, env=env)
    else:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key, sorting_key_start_value=sorting_key_value, env=env)

    response_df = pd.DataFrame(response)
    return response_df

def eod_price_impliedvols_query(primary_key_value, instrument = '', strike = 0, env='live'):
    '''Query EoD Price Implied Vols. Primary key is intraday timestamp.

     Args:
         primary_key_value(str):EoD. Example:'2022-09-12'
         sorting key(str):instrument and strike. Example:'BTCUSD-DRB-20220913-FUT|23000' or empty
         env('live'):environment. Example:'live'

     Return:
         eod price implied vols dataframe
     '''    

    table_name = env + '.market.eod.priceImpliedVols'
    primary_key = 'eod'
    sorting_key = 'instrumentStrike'

    sorting_key_value = instrument + '|' + str(strike) if (instrument and strike) else ''

    if not sorting_key_value:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, env=env)
    else:
        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key, sorting_key_start_value=sorting_key_value, env=env)

    response_df = pd.DataFrame(response)
    return response_df

def impliedvols_query(primary_key_value, sorting_key_value='', ts_as_primary_key=False, env='live'):
    '''Query Implied Vols. Primary key is instrument.

     Args:
         primary_key_value(str):Instrument. Example:'ETHUSD-DRB-20230331-FUT'
         sorting key(str):timestamp. Example: '2023-02-01-00'
         env('live'):environment. Example:'live'
         ts_as_primary_key: query format. Example: 'timestamp-instrument-index'

     Return:
         implied vols dataframe
     '''    

    table_name = env + '.market.impliedVols'
    
    if ts_as_primary_key:
        index_name = 'timestamp-instrument-index'
        primary_key = 'timestamp'
        sorting_key = 'instrument'
    else:
        primary_key = 'instrument'
        sorting_key = 'timestamp'

    if not sorting_key_value:
        response = query(table_name=table_name, index_name=index_name, primary_key=primary_key, primary_key_value=primary_key_value, env=env)
    else:
        response = query(table_name=table_name, index_name=index_name, primary_key=primary_key, primary_key_value=primary_key_value, sorting_key=sorting_key, sorting_key_start_value=sorting_key_value, env=env)
 

    response_df = pd.DataFrame(response)
    return response_df




def time_to_unix(time_str):
    u=calendar.timegm(datetime.datetime.strptime(time_str,'%Y-%m-%d-%H-%S').timetuple())*1000
    u_str=str(int(u))
    return u_str


def trades_query_all_accounts(start_time,end_time='',env='live'):
    df=pd.DataFrame()
    sorting_key_start_value=time_to_unix(start_time)
    if end_time != '':
        sorting_key_end_value=time_to_unix(end_time)
    else:
        sorting_key_end_value=str(round(time.time() * 1000))

    for item in ACCOUNT_DICT:
        primary_key_value=item

        table_name = 'live.portfolio.trades'
        primary_key = 'account'
        sorting_key = 'timestampInstrumentExchangeTypeId'

        response = query(table_name=table_name, primary_key=primary_key, primary_key_value=primary_key_value,
                         sorting_key=sorting_key,
                         sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value,
                         env=env)
        response_df = pd.DataFrame(response)
        if not response_df.empty:
            response_df['updatedAt'] = response_df['updatedAt'].apply(
                lambda x: (datetime.datetime.utcfromtimestamp(float(x) / 1000)).strftime('%Y-%m-%d %H:%M:%S'))
            response_df['timestamp'] = response_df['timestampInstrumentExchangeTypeId'].apply(
                lambda x: (datetime.datetime.utcfromtimestamp(int(x.split('|')[0]) / 1000)).strftime('%Y-%m-%d %H:%M:%S'))
            response_df['instrument'] = response_df['timestampInstrumentExchangeTypeId'].apply(
                lambda x: x.split('|')[1])
            response_df['exchangeType'] = response_df['timestampInstrumentExchangeTypeId'].apply(
                lambda x: x.split('|')[2])
            response_df['id'] = response_df['timestampInstrumentExchangeTypeId'].apply(
                lambda x: x.split('|')[3])

            response_df=response_df.drop(['timestampInstrumentExchangeTypeId'],axis=1)

        df=pd.concat([df,response_df],ignore_index=True)

    return df

def portfolios_scan( env="live" ):
    table_name = env + '.static.portfolios'
    dynamodb = dynamodb_init(env)
    table = dynamodb.Table(table_name)
    response = pd.DataFrame(table.scan()['Items'])
    return response

def bot_requests_query_from_db(bot, primary_key_value, sorting_key_start_value, sorting_key_end_value='', filter_key='', filter_operation='', filter_key_start_value='', filter_key_end_value='', env='live'):
    table_name = env + '.requests.' + bot
    primary_key = 'portfolio'
    sorting_key = 'timestampId'
    df_portfolios = portfolios_scan().set_index('name')
    df_users = users_scan().set_index('id')
    response = query(table_name=table_name, primary_key=primary_key, primary_key_value=df_portfolios.loc[primary_key_value, 'id'], sorting_key=sorting_key, sorting_key_start_value=sorting_key_start_value, sorting_key_end_value=sorting_key_end_value, filter_key=filter_key, filter_operation=filter_operation, filter_key_start_value=filter_key_start_value, filter_key_end_value=filter_key_end_value, env=env)
    response_df = pd.DataFrame(response)
    if not response_df.empty:
        response_df['updatedAt'] = response_df['updatedAt'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
        response_df['timestamp'] = response_df['timestampId'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x.split('|')[0])/1000).strftime('%Y-%m-%d %H:%M:%S'))
        response_df['id'] = response_df['timestampId'].apply(lambda x: x.split('|')[1])
        response_df['user'] = response_df['user'].apply(lambda x: df_users.loc[x,'email'].split('@')[0])
        response_df['portfolio'] = primary_key_value
        
    return response_df
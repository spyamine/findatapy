from findatapy.market.ioenginegenerator import IOEngineGenerator

if __name__ == '__main__':
    import ystockquote
    import collections
    import pandas as pd
    from datetime import datetime as dt
    import pandas


    def get_stock_history(ticker, start_date, end_date):
        data = ystockquote.get_historical_prices(ticker, start_date, end_date)
        df = pandas.DataFrame(collections.OrderedDict(sorted(data.items()))).T
        df = df.convert_objects(convert_numeric=True)
        return df


    engine = IOEngineGenerator().load_engine("Arctic")

    host = '127.0.0.1'
    library = 'sto'
    symbol = 'aapl'
    key1 = {'source': 'Yahoo'}

    print "getting data for " + symbol
    df = get_stock_history(symbol, '2015-01-01', '2015-02-01')

    print "print writing data to the DB"
    engine.write(symbol, df, library, metadata=key1)

    print "reading data from the DB"
    print engine.read(symbol, library)

    print "getting new data"
    df2 = get_stock_history(symbol, '2015-02-01', '2015-03-01')

    print "wrinting the new data into the database "
    engine.write(symbol, df2, library, append_data=True)

    print "Reading the full data"
    print engine.read(symbol, library)

    symbol = 'c'
    key1 = {'source': 'Yahoo'}

    print "getting data for " + symbol
    df = get_stock_history(symbol, '2015-01-01', '2016-02-01')

    print "print writing data to the DB"
    engine.write(symbol, df, library, metadata=key1)

    print "Reading the full data"
    print engine.read(symbol, library)

    # print engine.list_symbols(library,key2)
    print "list libraries"
    print engine.list_libraries()
    print "listing available symbols"
    print engine.list_symbols(library)

    print "testing delete"
    print "deleting " + symbol + " from library"
    engine.delete_symbol(symbol, library)

    print "checking the results "
    print engine.list_symbols(library)

    print "test has-symbol"
    print engine.has_symbol(symbol, library)

    symbol = 'aapl'
    print engine.has_symbol(symbol, library)

    print "test delete library "
    engine.delete_library(library)

    print "remain libraries"
    print engine.list_libraries()

__author__ = 'Mohamed Amine Guessous' # Mohamed Amine Guessous

#
# Copyright 2016 
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#

"""
Write and reads time series data to disk through Arctic library
"""

from findatapy.market.ioengine import IOEngine
from findatapy.util.loggermanager import LoggerManager


class IOEngineArctic(IOEngine):



    def __init__(self):

        super(IOEngine, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)
        return

    def write(self, symbol, data_frame,library_name , append_data = False, metadata = None,   host = '127.0.0.1' ):
        """

        :param symbol: name of the data
        :param data_frame: data to be saved
        :param host: equivalent to DB for SQL, path for CSV, Excel and HD5, store in Arctic
        :param library: equivalent to table, library in Arctic
        :param append_data:
        :return:
        """
        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo


        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)
        self.logger.info("Connect to the mongo-host / clustera" + host)

        #delete the store if available because we rewrite the data
        #store.delete_library

        #test to see if we have already the data
        database = None

        try:
            database = store[library_name]
        except:
            pass

        if database is None:
            store.initialize_library(library_name)
            self.logger.info("Created MongoDB library: " + library_name)
        else:
            self.logger.info("Got MongoDB library: " + library_name)

        # Access the library
        library = store[library_name]

        if ('intraday' in library_name):
            data_frame = data_frame.astype('float32')

        # can duplicate values if we have existing dates
        if metadata is None:
            if append_data:
                library.append(symbol, data_frame)
            else:
                library.write(symbol, data_frame)
            self.logger.info("Written MongoDB library: " + symbol)
        elif isinstance(metadata,dict):
            if append_data:
                library.append(symbol, data_frame,metadata=metadata)
            else:
                library.write(symbol, data_frame, metadata= metadata)
            self.logger.info("Written MongoDB library: " + symbol)

        else:
            self.logger.info("metadata format not correct, check if it's a dictionary " )

        c.close()

        return

    def read(self, symbol , library , start_date = None, finish_date = None,  metadata = None, host = '127.0.0.1' ):
        """

        :param symbol: name of the data
        :param library: equivalent to table, library in Arctic
        :param host: equivalent to DB for SQL, path for CSV, Excel and HD5, store in Arctic
        :return: dataframe of the data requested
        """
        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)

        self.logger.info("Connect to the mongo-host / clustera " + host)

        # test to see if we have already the data
        database = None

        try:
            database = store[library]
        except:
            self.logger.info("library not available")
            return

        # Access the library
        library = store[library]
        if metadata == None:
            if start_date is None and finish_date is None:
                item = library.read(symbol)
            else:
                from arctic.date import DateRange
                item = library.read(symbol, date_range=DateRange(start_date, finish_date))
        else :
            if start_date is None and finish_date is None:
                item = library.read(symbol,metadata=metadata)
            else:
                from arctic.date import DateRange
                item = library.read(symbol, date_range=DateRange(start_date, finish_date),metadata=metadata)



        self.logger.info('Read ' + symbol)

        c.close()

        return item.data


    def delete_symbol(self,symbol , library , host = '127.0.0.1' ):
        """

        :param symbol:
        :param library:

        :param host:
        :return:
        """

        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)

        self.logger.info("Connect to the mongo-host / clustera " + host)

        # test to see if we have already the data
        database = None

        try:
            database = store[library]
        except:
            self.logger.info("library not available")
            return

        # Access the library
        library = store[library]

        item = library.delete(symbol)

        self.logger.info('delete ' + symbol)
        c.close()

        return

    def delete_library(self, library, host='127.0.0.1'):
        """

        :param symbol:
        :param library:

        :param host:
        :return:
        """

        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)

        self.logger.info("Connect to the mongo-host / clustera " + host)

        # test to see if we have already the data
        database = None

        try:
            store.delete_library(library)
        except:
            self.logger.info("library not available")
            return

        self.logger.info('delete ' + library)
        c.close()

        return

    def list_symbols(self, library, key = None , host = '127.0.0.1'):
        """

        :param library:
        :param key:
        :param host:
        :return:
        """

        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)
        self.logger.info("Connect to the mongo-host / clustera " + host)

        # test to see if we have already the data
        database = None

        try:
            database = store[library]
        except:
            self.logger.info("store not available")
            c.close()
            return

        # Access the library
        library = store[library]

        # What symbols (keys) are stored in the library
        if key is None:
            symbols = library.list_symbols()
            c.close()
            return symbols
        else :
            if isinstance(key,dict):
                symbols = library.list_symbols(metadata = key)
                c.close()
                return symbols
            else:
                return


    def list_libraries(self, host = '127.0.0.1'):
        """

        :param host:
        :return:
        """

        try:
            from arctic import Arctic
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)
        self.logger.info("Connect to the mongo-host / clustera " + host)

        self.logger.info('List of MongoDB libraries in : ' + host)
        list_lib= store.list_libraries()
        c.close()
        return list_lib

    def has_symbol(self, symbol, library, host='127.0.0.1'):

        """

        :param symbol:
        :param library:
        :param host:
        :return:
        """


        try:
            from arctic import Arctic
            from datetime import datetime as dt
            import pandas as pd
            import pymongo
        except:
            pass

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)

        self.logger.info("Connect to the mongo-host / clustera " + host)

        # test to see if we have already the data
        database = None

        try:
            database = store[library]
        except:
            self.logger.info("library not available")
            return

        # Access the library
        library = store[library]

        exists = library.has_symbol(symbol)

        self.logger.info('Check if ' + symbol + ' exists')
        c.close()

        return exists
